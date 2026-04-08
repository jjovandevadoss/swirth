#!/usr/bin/env python3
"""
Parse a sample Sysmex H550 ASTM LIS2-A2 message using the dynamic Giga parser.
"""

import json
from parsers.giga_parser import GigaParser

SAMPLE_MESSAGE = r"""H|\^&|||H550^211YADH04038^4.0.2.3|||||||P|LIS2-A2|20260402122236
P|1
O|1|SAJAN SINGH^^038749^1||^^^DIF|R|20260402112818|||||X||||BLOOD||||||||||F
C|1|I|CONDITIONS^^MANUAL_MATCH\S^RBC_OTH^PLT_ABN_HIST^SCH_MACRO_PLT\SUSPECTED_PATHOLOGY^^LYMPHOPENIA\SUSPECTED_PATHOLOGY^^NEUTROPENIA|I
M|1|REAGENT|CLEANER\DILUENT\LYSE|251205I17^20260401000000^20260701\251120H17^20260326000000^20260926\251021M11^20260327000000^20260527
M|2|SETTING|RUO\WBCDIFF|TRUE\5
R|1|^^^WBC^6690-2|2.15|1E03/mm3|4.00 - 11.00^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|2|^^^RBC^789-8|3.89|1E06/mm3|3.90 - 5.80^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|3|^^^HGB^718-7|10.2|g/dL|11.5 - 16.7^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|4|^^^HCT^4544-3|32.4|%|35.0 - 49.0^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|5|^^^MCV^787-2|83.3|fL|75.0 - 97.0^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|6|^^^MCH^785-6|26.1|pg|26.5 - 33.0^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|7|^^^MCHC^786-4|31.4|g/dL|32.0 - 36.0^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|8|^^^RDW-CV^788-0|16.1|%|12.0 - 18.0^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|9|^^^RDW-SD^21000-5|33.6|fL|37.0 - 56.0^REFERENCE_RANGE|LL||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|10|^^^PLT^777-3|120|1E03/mm3|150 - 450^REFERENCE_RANGE|L||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|11|^^^PDW^51631-0|19.5|fL|11.0 - 20.0^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|12|^^^PCT^51637-7|0.131|%|0.150 - 0.400^REFERENCE_RANGE|L||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|13|^^^MPV^32623-1|10.9|fL|7.4 - 11.0^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|14|^^^P-LCC^96354-6|54|1E03/mm3|44 - 140^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|15|^^^P-LCR^48386-7|44.7|%|18.0 - 50.0^REFERENCE_RANGE|N||W||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|16|^^^LYM#^731-0|0.79|1E03/mm3|1.25 - 4.00^REFERENCE_RANGE|LL||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|17|^^^LYM%^736-9|36.6|%|15.0 - 45.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|18|^^^MON#^742-7|0.12|1E03/mm3|0.20 - 0.80^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|19|^^^MON%^5905-5|5.4|%|4.0 - 13.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|20|^^^NEU#^751-8|1.13|1E03/mm3|1.50 - 7.50^REFERENCE_RANGE|LL||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|21|^^^NEU%^770-8|53.2|%|40.0 - 75.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|22|^^^EOS#^711-2|0.10|1E03/mm3|0.00 - 0.40^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|23|^^^EOS%^713-8|4.4|%|0.5 - 7.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|24|^^^BAS#^704-7|0.01|1E03/mm3|0.00 - 0.10^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|25|^^^BAS%^706-2|0.4|%|0.0 - 2.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|26|^^^LIC#^55432-9|0.00|1E03/mm3|0.00 - 0.20^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|27|^^^LIC%^55433-7|0.1|%|0.0 - 3.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|28|^^^ALY#^43743-4|0.02|1E03/mm3|0.00 - 0.20^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|29|^^^ALY%^42250-1|0.9|%|0.0 - 2.5^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|30|^^^MIC^X-MIC|7.5|%|0.0 - 20.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|31|^^^MAC^X-MAC|1.6|%|2.0 - 10.0^REFERENCE_RANGE|L||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|32|^^^IMM#^X-IMM#|0.00|1E03/mm3|0.00 - 0.10^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|33|^^^IML#^X-IML#|0.00|1E03/mm3|0.00 - 0.05^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|34|^^^IMG#^53115-2|0.00|1E03/mm3|0.00 - 0.50^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|35|^^^IMM%^X-IMM%|0.0|%|0.0 - 0.5^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|36|^^^IML%^X-IML%|0.0|%|0.0 - 0.2^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
R|37|^^^IMG%^71695-1|0.0|%|0.0 - 2.0^REFERENCE_RANGE|N||F||CHCC^^USER|20260402112818|20260402112818|211YADH04038
L|1|N"""


def main():
    parser = GigaParser()
    parsed = parser.parse(SAMPLE_MESSAGE)

    # --- Header ---
    header = parsed.get('header', {})
    instrument = parsed.get('instrument', {})
    instrument_model = instrument.get('model') or 'Unknown'
    instrument_serial = instrument.get('serial') or 'Unknown'
    firmware = instrument.get('firmware') or 'Unknown'
    message_profile = parsed.get('message_profile') or 'Unknown'

    print("=" * 70)
    print(f"  Sysmex {instrument_model}  |  Serial: {instrument_serial}  |  FW: {firmware}")
    print(f"  Protocol: {header.get('version')}  |  Processing: {header.get('processing_id')}")
    print(f"  Message Time: {header.get('timestamp')}  |  Profile: {message_profile}")
    print("=" * 70)

    # --- Patient / Order ---
    patient = parsed.get('patient', {})
    patient_name = 'Unknown'
    patient_id = 'N/A'
    name_dict = patient.get('name')
    if name_dict:
        name_parts = [name_dict.get('last'), name_dict.get('first'), name_dict.get('middle')]
        patient_name = ' '.join(p for p in name_parts if p)
    patient_id = patient.get('practice_patient_id') or patient.get('lab_patient_id') or 'N/A'

    print(f"\n  Patient Name: {patient_name}")
    print(f"  Patient ID:   {patient_id}")

    for order in parsed.get('orders', []):
        sid = order.get('specimen_id_parsed', {})
        accession = sid.get('accession_number', '') if sid else ''

        test_id = order.get('universal_test_id', {}) or {}
        test_mnemonic = test_id.get('mnemonic') or test_id.get('test_name') or test_id.get('test_code') or test_id.get('test_id') or test_id.get('manufacturer') or ''
        test_display = test_id.get('display_name') or test_mnemonic

        ordered_dt = order.get('ordered_datetime') or order.get('collection_datetime') or 'N/A'

        print(f"  Accession #:  {accession}")
        print(f"  Test Profile: {test_display}")
        print(f"  Specimen:     {order.get('specimen_descriptor') or 'N/A'}")
        print(f"  Priority:     {order.get('priority') or 'N/A'}")
        print(f"  Report Type:  {order.get('report_type') or 'N/A'}")
        print(f"  Ordered:      {ordered_dt}")

    # --- Results Table ---
    results = parsed.get('results', [])
    if results:
        print(f"\n  {'#':<4} {'Test':<30} {'Value':>10} {'Units':<12} {'Ref Range':<20} {'Flag':<6} {'Status'}")
        print("  " + "-" * 100)
        for r in results:
            tid = r.get('universal_test_id', {}) or {}
            test_display = tid.get('display_name') or tid.get('mnemonic') or tid.get('test_name') or '?'
            value = r.get('value', '')
            units = r.get('units', '')
            ref = r.get('reference_range_parsed', {}) or {}
            if 'min' in ref and 'max' in ref:
                ref_str = f"{ref['min']} - {ref['max']}"
            else:
                ref_str = ref.get('raw', '')
            flag = r.get('abnormal_flags', '')
            status = r.get('result_status', '')
            seq = r.get('sequence', '')

            # Highlight abnormal flags
            flag_marker = ''
            if flag and flag not in ('N',):
                flag_marker = f" {'*' if flag == 'L' else '**' if flag == 'LL' else '!' if flag == 'H' else '!!' if flag == 'HH' else flag}"

            print(f"  {seq:<4} {test_display:<30} {value:>10} {units:<12} {ref_str:<20} {flag:<6} {status}{flag_marker}")

    # --- Comments ---
    for comment in parsed.get('comments', []):
        print(f"\n  Comment (source={comment.get('comment_source')}, type={comment.get('comment_type')}):")
        print(f"    {comment.get('comment_text')}")

    # --- Manufacturer Records ---
    for mfr in parsed.get('manufacturer_records', []):
        print(f"\n  Manufacturer [{mfr.get('definition_scope')}] {mfr.get('definition_name')}:")
        for extra in (mfr.get('implementation_fields') or []):
            if extra:
                print(f"    {extra}")

    # --- Terminator ---
    term = parsed.get('terminator', {})
    print(f"\n  Termination: {term.get('termination_code')} (Normal)" if term.get('termination_code') == 'N' else '')

    # --- Full JSON dump ---
    print("\n" + "=" * 70)
    print("  Full parsed JSON:")
    print("=" * 70)
    print(json.dumps(parsed, indent=2, default=str))


if __name__ == '__main__':
    main()
