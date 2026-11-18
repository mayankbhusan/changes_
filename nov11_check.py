#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
nov11_check.py - read-only checks for a BA Excel vs the PR notes file vs the ingestion config.

  python3 nov11_check.py --xlsx UAT_Orch_EBBS_Nov11th_PR2.xlsx --notes NOV11_PR2.txt \
        [--country FK] [--sheet PR] [--ingestion-dir DIR] [--product-dir DIR]

Task 1: every row / every "Dependent Tables" entry of the Excel must be in the block of the notes file.
Task 2: every dependency of every block in the notes file must exist in ebbs_ingestion_config.csv
        (services / products / reference tables are listed separately, they are not EBBS tables).
Nothing is written anywhere.
"""

import argparse
import json
import os
import re
import sys
import tempfile
import zipfile
import xml.etree.ElementTree as ET

DEF_PRODUCT_DIR = os.environ.get(
    'DEP_PRODUCT_DIR',
    '/CTRLFW/FDP/dev/tools_v2/executables/airflow_home/dag_config_uat_refresh/product')
DEF_ING_DIR = os.environ.get(
    'DEP_ING_DIR',
    '/CTRLFW/FDP/uat/tools_v2/executables/airflow_home/dag_config/ingestion')

NS = '{http://schemas.openxmlformats.org/spreadsheetml/2006/main}'
RNS = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}'


# --------------------------------------------------------------------------
# xlsx reading (stdlib only)
# --------------------------------------------------------------------------
def _col_idx(ref):
    letters = re.match(r'[A-Z]+', ref).group(0)
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n - 1


def read_xlsx(path, sheet=None):
    z = zipfile.ZipFile(path)
    shared = []
    if 'xl/sharedStrings.xml' in z.namelist():
        root = ET.fromstring(z.read('xl/sharedStrings.xml'))
        for si in root.findall(NS + 'si'):
            shared.append(''.join(t.text or '' for t in si.iter(NS + 't')))
    wb = ET.fromstring(z.read('xl/workbook.xml'))
    rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
    rmap = dict((r.get('Id'), r.get('Target')) for r in rels)
    sheets = [(s.get('name'), rmap[s.get(RNS + 'id')]) for s in wb.find(NS + 'sheets')]
    if sheet is None:
        name, target = sheets[0]
    else:
        m = [s for s in sheets if s[0].lower() == sheet.lower()]
        if not m:
            raise SystemExit('Sheet %r not found. Sheets in file: %s'
                             % (sheet, ', '.join(s[0] for s in sheets)))
        name, target = m[0]
    target = target.lstrip('/')
    if not target.startswith('xl/'):
        target = 'xl/' + target
    root = ET.fromstring(z.read(target))
    rows = []
    for r in root.iter(NS + 'row'):
        cells = {}
        for c in r.findall(NS + 'c'):
            t = c.get('t')
            v = c.find(NS + 'v')
            if t == 'inlineStr':
                is_ = c.find(NS + 'is')
                val = ''.join(x.text or '' for x in is_.iter(NS + 't')) if is_ is not None else ''
            elif v is None:
                continue
            elif t == 's':
                val = shared[int(v.text)]
            else:
                val = v.text or ''
            cells[_col_idx(c.get('r'))] = val
        rows.append((int(r.get('r')), cells))
    return rows


def _norm(s):
    return re.sub(r'\s+', ' ', (s or '').strip().lower())


HEADERS = {
    'product': ['fdp data product'],
    'service': ['service name/process_id_val', 'service name', 'process_id_val'],
    'status': ['existing/new'],
    'country': ['country_val', 'country'],
    'tp': ['tp_sys_val'],
    'deps': ['dependent tables', 'dependencies'],
}


def load_sheet_rows(path, sheet):
    raw = read_xlsx(path, sheet)
    hdr = None
    for i, (rn, cells) in enumerate(raw):
        if any(_norm(v) == 'dependent tables' for v in cells.values()):
            hdr = i
            break
    if hdr is None:
        raise SystemExit('Header row containing "Dependent Tables" not found in sheet.')
    header = dict((_norm(v), c) for c, v in raw[hdr][1].items())
    cols = {}
    for k, names in HEADERS.items():
        cols[k] = next((header[n] for n in names if n in header), None)
    for k in ('product', 'service', 'country', 'deps'):
        if cols[k] is None:
            raise SystemExit('Column for %r not found. Headers seen: %s' % (k, sorted(header)))
    out = []
    for rn, cells in raw[hdr + 1:]:
        g = lambda k: (cells.get(cols[k]) or '').strip() if cols[k] is not None else ''
        if not g('service'):
            continue
        out.append(dict(row=rn, product=g('product'), service=g('service'), status=g('status'),
                        country=g('country'), tp=g('tp') or 'EBBS', deps=cells.get(cols['deps']) or ''))
    return out



DEF_PRODUCT_DIR = '/CTRLFW/FDP/dev/tools_v2/executables/airflow_home/dag_config_uat_refresh/product'
DEF_ING_DIR = '/CTRLFW/FDP/uat/tools_v2/executables/airflow_home/dag_config/ingestion'

BLOCK_RE = re.compile(r'"([^"\n]+)"\s*:\s*\{\s*"dependencies"\s*:\s*"([^"]*)"\s*,\s*"partition_details"\s*:\s*"([^"]*)"\s*\}')
DAAS_RE = re.compile(r'^fdp_daas_\$\{env\}\.(\w+)$', re.I)
DF_RE = re.compile(r'^fdp_df_([a-z0-9]+)_sri_open_\$\{env\}\.(\w+)$', re.I)       # FDP_DF_ALL_SRI_OPEN_${env}.EBBS_X
SRC_RE = re.compile(r'^fdp_([a-z0-9]+)_([a-z0-9]+)_sri_open_\$\{env\}\.(\w+)$', re.I)  # fdp_ebbs_all_sri_open_${env}.EBBS_X
ING_STYLE = re.compile(r'^[A-Z0-9]+_(DF_)?[A-Z]{2}_[A-Z0-9]+_\w+$')


def split_deps(raw):
    out = []
    for part in re.split(r'[\n;,]+', raw or ''):
        t = part.strip().strip('"\'').strip()
        if t and t not in out:
            out.append(t)
    return out


def expected_token(tok, cc):
    """Excel token -> (value, kind, alternatives).
    <prefix>_${env}.<name>:
      fdp_daas_${env}.X                         -> X                      (product)
      FDP_DF_<SYS>_ALL_SRI_OPEN_${env}.<name>   -> <SYS>_DF_<CC>_<name>   (data factory)
      fdp_<sys>_all_sri_open_${env}.<name>      -> <SYS>_<CC>_<name> or <SYS>_<CC>_<SYS>_<CC>_<rest> (plain)
      <anything>_${env}.M_<name>                -> M_<name>               (reference table, kept as is)
      <name>_ALL                                -> <name>_<CC>            (service)
    Anything after a space inside a token is stray text and is ignored."""
    tok = tok.split()[0] if tok.split() else tok
    if '_${env}.' in tok:
        prefix, name = tok.split('_${env}.', 1)
        pu, nu = prefix.upper(), name.upper()
        if pu == 'FDP_DAAS':
            return name, 'product', [name]
        if nu.startswith('M_'):
            return nu, 'reference', [nu]
        m = re.match(r'^FDP_DF_([A-Z0-9]+)_ALL_SRI_OPEN$', pu)
        if m:
            v = '%s_DF_%s_%s' % (m.group(1), cc, nu)
            return v, 'ingestion', [v]
        m = re.match(r'^FDP_([A-Z0-9]+)_ALL_SRI_OPEN$', pu)
        if m:
            sysn = m.group(1)
            rest = nu[len(sysn) + 1:] if nu.startswith(sysn + '_') else nu
            a_ = '%s_%s_%s' % (sysn, cc, nu)
            b_ = '%s_%s_%s_%s_%s' % (sysn, cc, sysn, cc, rest)
            return a_, 'ingestion', [b_, a_]
        return nu, 'name', [nu]
    if tok.endswith('_ALL'):
        v = tok[:-4] + '_' + cc
        return v, 'service', [v]
    return tok, 'name', [tok]


def load_ing(ing_dir):
    ebbs, other = set(), {}
    for fn in sorted(os.listdir(ing_dir)):
        if not fn.endswith('ingestion_config.csv'):
            continue
        with open(os.path.join(ing_dir, fn), 'rb') as f:
            for line in f.read().decode('utf-8', 'replace').splitlines():
                k = line.split('|')[0].strip()
                if not k:
                    continue
                if fn == 'ebbs_ingestion_config.csv':
                    ebbs.add(k)
                else:
                    other.setdefault(k, fn)
    return ebbs, other


def load_prod_keys(prod_dir):
    keys, files = set(), set()
    if not os.path.isdir(prod_dir):
        return keys, files
    for fn in os.listdir(prod_dir):
        if fn.endswith('_dependency_config.json'):
            files.add(fn[:-len('_dependency_config.json')].upper())
            txt = open(os.path.join(prod_dir, fn), 'rb').read().decode('utf-8', 'replace')
            for m in re.finditer(r'"([^"\n]+)"\s*:\s*\{\s*"dependencies"', txt):
                keys.add(m.group(1))
    return keys, files


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--xlsx', required=True)
    ap.add_argument('--notes', required=True)
    ap.add_argument('--sheet', default='PR')
    ap.add_argument('--country', default=None, help='override Country_Val from the sheet (e.g. FK)')
    ap.add_argument('--ingestion-dir', default=DEF_ING_DIR)
    ap.add_argument('--product-dir', default=DEF_PRODUCT_DIR)
    a = ap.parse_args()

    rows = load_sheet_rows(a.xlsx, a.sheet)
    text = open(a.notes, 'rb').read().decode('utf-8', 'replace')
    blocks = {}
    dup = []
    for m in BLOCK_RE.finditer(text):
        k = m.group(1)
        if k in blocks:
            dup.append(k)
        blocks[k] = dict(deps=[x.strip() for x in m.group(2).split(',') if x.strip()], part=m.group(3))
    ebbs, other = load_ing(a.ingestion_dir)
    pkeys, pfiles = load_prod_keys(a.product_dir)

    print('Excel rows: %d | blocks found in %s: %d | EBBS ingestion keys: %d' % (len(rows), os.path.basename(a.notes), len(blocks), len(ebbs)))
    if dup:
        print('!! block appears more than once in the notes file: ' + ', '.join(sorted(set(dup))))
    if not blocks:
        print('!! no "KEY": { "dependencies": ..., "partition_details": ... } blocks could be read from the notes file')

    # ---------------- task 1 ----------------
    print('\n==================== TASK 1: Excel -> notes file ====================')
    t1_bad = 0
    used = set()
    for r in rows:
        cc = (a.country or r['country']).upper()
        svc = r['service']
        key = (svc[:-4] if svc.endswith('_ALL') else svc) + '_' + cc
        print('\nRow %d  %s  ->  %s' % (r['row'], r['product'], key))
        b = blocks.get(key)
        if b is None:
            near = [k for k in blocks if k.startswith(svc[:-4] if svc.endswith('_ALL') else svc)]
            print('    FAIL   block %s not in notes file%s' % (key, ('  (similar: %s)' % ', '.join(near)) if near else ''))
            t1_bad += 1
            continue
        used.add(key)
        p = re.search(r'process_id_val=([^;"]*)', b['part'])
        c = re.search(r'country_val=([^;"]*)', b['part'])
        if not p or p.group(1) != svc:
            print('    FAIL   process_id_val is %s, Excel says %s' % (p and p.group(1), svc)); t1_bad += 1
        if not c or c.group(1) != cc:
            print('    FAIL   country_val is %s, expected %s' % (c and c.group(1), cc)); t1_bad += 1
        have = set(b['deps'])
        exp_vals = []
        for tok in split_deps(r['deps']):
            val, kind, alts = expected_token(tok, cc)
            if len(tok.split()) > 1:
                print('    NOTE   Excel entry has stray text after a space, ignored: %r' % tok)
            hit = next((x for x in alts if x in have), None)
            exp_vals.extend(alts)
            if hit:
                print('    OK     %-46s <- %s' % (hit, tok))
                continue
            other_style = None
            if kind == 'ingestion':
                if '_DF_' in val:    # Excel says DF -> maybe the notes file has the plain name
                    sysn = val.split('_DF_')[0]
                    nm = val.split('_DF_%s_' % cc)[1]
                    rest = nm[len(sysn) + 1:] if nm.startswith(sysn + '_') else nm
                    cands = ['%s_%s_%s' % (sysn, cc, nm), '%s_%s_%s_%s_%s' % (sysn, cc, sysn, cc, rest)]
                else:                # Excel says plain -> maybe the notes file has the DF name
                    sysn = alts[0].split('_')[0]
                    nm = alts[1][len('%s_%s_' % (sysn, cc)):] if len(alts) > 1 else alts[0][len('%s_%s_' % (sysn, cc)):]
                    cands = ['%s_DF_%s_%s' % (sysn, cc, nm)]
                other_style = next((x for x in cands if x in have), None)
            if other_style:
                print('    STYLE  %-46s <- %s   (notes file has %s instead)' % (val, tok, other_style)); t1_bad += 1
            else:
                print('    MISSING %-45s <- %s' % (alts[0], tok)); t1_bad += 1
        extra = [d for d in b['deps'] if d not in exp_vals]
        if extra:
            print('    INFO   in notes file but not in the Excel row: ' + ', '.join(extra))
    unused = [k for k in blocks if k not in used]
    if unused:
        print('\nINFO   blocks in the notes file that match no Excel row: ' + ', '.join(unused))
    print('\nTASK 1 result: %d problem(s)' % t1_bad)

    # ---------------- task 2 ----------------
    print('\n==================== TASK 2: notes file -> ebbs_ingestion_config.csv ====================')
    t2_bad = 0
    svc_names = set(blocks) | pkeys
    for k, b in blocks.items():
        print('\n%s' % k)
        for d in b['deps']:
            if d in ebbs:
                print('    FOUND      %s' % d)
            elif ING_STYLE.match(d) and d.split('_')[0] == 'EBBS':
                print('    NOT FOUND  %s   (EBBS-style table name, not in ebbs_ingestion_config.csv)' % d); t2_bad += 1
            elif d in other:
                print('    other CSV  %s   (found in %s, not an EBBS table)' % (d, other[d]))
            elif d in svc_names:
                print('    service    %s   (block in notes file / product config)' % d)
            elif d.upper() in pfiles:
                print('    product    %s   (%s_dependency_config.json exists)' % (d, d.lower()))
            else:
                print('    UNVERIFIED %s   (not an EBBS ingestion key, service or product: reference table? check manually)' % d)
    print('\nTASK 2 result: %d EBBS table(s) NOT FOUND' % t2_bad)


if __name__ == '__main__':
    main()
