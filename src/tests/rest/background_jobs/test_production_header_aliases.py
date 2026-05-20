"""Production sheet row-1 headers must all resolve via _FIELD_SPECS aliases."""
from __future__ import annotations

import unittest

from background_jobs import dispatch_sync as ds

PRODUCTION_HEADER_ROW = {
    "column_A": "SrNo",
    "column_B": "DC# No",
    "column_C": "DC Date",
    "column_D": "Account Name",
    "column_E": "Products",
    "column_F": "Terms",
    "column_G": "Qty",
    "column_H": "Amount",
    "column_I": "PO#",
    "column_J": "PO Date",
    "column_K": "Engg",
    "column_L": "SalesOrder#",
    "column_M": "Consignee City",
    "column_N": "Serial #",
    "column_R": "Remarks",
    "column_S": "DC Recd in office",
    "Godown_O1": "Date of Material Dispatch",
    "Godown_P1": "Date of Dispatch of Godown DC to office",
    "Godown_Q1": "Date of Scanned Copy DC sent to Office",
    "Godown_U1": "E Way Bill No",
    "Godown_W1": "Transporter / Courier Name",
    "Godown_X1": "Vehicle No",
    "GODOWN_AU1": "IN Time",
    "GODOWN_AV1": "OUT Time",
    "Godown_AD1": "Date of dispatch of L/R to office",
    "Godown___Check": "E Way Updated in Server",
    "ArvindG_Y1": "L/R No",
    "ArvindG_Z1": "L/R Date",
    "ArvindG_AA1": "Freight Mode",
    "ArvindG_AB1": "Freight Amount",
    "ArvindG_AC1": "Date of Delivery at Consignee",
    "ArvindG_AF1": "Date of Email to customer with vehicle / dispatch details",
    "Umesh_AE1": "LR Recd in office",
    "Tulsi_AI1": "Date of Email to customer with Inv Details",
    "Tulsi_AJ1": "Date of Email to customer with TC Details",
    "Tulsi_AK1": "Date of Courier sent to customer",
    "Umesh_AL1": "SIS/CTF Pump Model",
    "Umesh_AM1": "Model Serial No.",
    "Umesh_AN1": "SIS/CTF CRM No",
    "Umesh_AO1": "SIS/CTF Date",
    "Umesh_AP1": "SIS CTF DONE",
    "Umesh_AQ1": "SIS -CTF MAIL",
    "column_AH": "E-Warranty #",
    "Akshay": "E-Warranty updated date",
    "Umesh_Akshay": "DC in office",
    "column_AR": "NOTE",
    "DarshanS_AS1": "CHECKED/GATHER",
    "DarshanS_AT1": "BARCODE",
}


class ProductionHeaderAliasTests(unittest.TestCase):
    def test_all_production_header_labels_map(self):
        header_map = ds._mapping_from_header_row(PRODUCTION_HEADER_ROW)
        unmapped = [
            (col, label, ds._normalize_header_label(label))
            for col, label in PRODUCTION_HEADER_ROW.items()
            if col not in header_map
        ]
        self.assertEqual(unmapped, [], f"Unmapped headers: {unmapped}")

    def test_merged_mapping_includes_all_legacy_physical_columns(self):
        full = ds._build_column_mapping(PRODUCTION_HEADER_ROW)
        legacy = ds._legacy_column_mapping()
        self.assertEqual(len(full), len(legacy))
        for col in legacy:
            self.assertIn(col, full)
