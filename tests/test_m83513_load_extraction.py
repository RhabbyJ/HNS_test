import unittest

from postgresql.m83513_load_extraction import base_rows_for_extraction, wire_rows_for_base


CLASS_M_FINISHES = [
    {"code": code, "description": description}
    for code, description in [
        ("A", "Pure electrodeposited aluminum"),
        ("C", "Cadmium"),
        ("K", "Zinc nickel"),
        ("N", "Electroless nickel"),
        ("P", "Passivated stainless steel"),
        ("T", "Nickel fluorocarbon polymer"),
    ]
]

INSERTS_8 = list(zip(["A", "B", "C", "D", "E", "F", "G", "H"], [9, 15, 21, 25, 31, 37, 51, 100], strict=False))
INSERTS_10 = list(zip(["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"], [9, 15, 21, 25, 31, 37, 51, 100, 100, 100], strict=False))
INSERTS_7 = list(zip(["A", "B", "C", "D", "E", "F", "G"], [9, 15, 21, 25, 31, 37, 51], strict=False))


def wire_options(count: int) -> list[dict]:
    return [
        {
            "wire_type_code": f"{index:02d}",
            "wire_specification": "M22759/11-26-9",
            "wire_length_inches": 18,
            "note_texts": [],
            "is_space_approved": False,
        }
        for index in range(1, count + 1)
    ]


def extraction_fixture(
    document_key: str,
    inserts: list[tuple[str, int]],
    *,
    finishes: list[dict] | None = None,
    components: list[str] | None = None,
    wire_count: int = 0,
    class_code: str = "M",
) -> dict:
    unique_cavities = sorted({cavity for _, cavity in inserts})
    return {
        "source": {
            "spec_sheet": f"MIL-DTL-83513/{int(document_key)}X",
            "document_key": document_key,
            "document_type": "plug_receptacle",
            "title": "Test connector",
            "source_url": "https://example.test",
            "storage_path": "mil-dtl-83513/test.pdf",
            "revision": "X",
        },
        "connector_type": "SIGNAL_CONNECTOR",
        "confidence_score": 1.0,
        "pin_components": {
            "prefix": f"M83513/{int(document_key):02d}",
            "components": components or ["insert_arrangement", "shell_finish_code"],
            "insert_arrangements": [
                {"insert_arrangement": code, "cavity_count": cavity}
                for code, cavity in inserts
            ],
            "shell_finish_options": finishes or [],
        },
        "configuration_rows": [
            {
                "page_number": 4,
                "cavity_count": cavity,
                "shell_size_letter": "C" if cavity == 100 else "B" if cavity == 51 else "A",
                "dimensions": {"unit": "inch", "A": float(cavity)},
            }
            for cavity in unique_cavities
        ],
        "attributes": {
            "shell_material": "Plastic" if class_code == "P" else "Metal",
            "class": class_code,
            "contact_type": "Socket",
            "gender": "Receptacle",
            "termination_style": "Crimp" if wire_count else "Solder",
            "polarization": "Standard polarized shell",
        },
        "mates_with": [],
        "figure_references": [],
        "wire_options": wire_options(wire_count),
    }


class LoaderRowGenerationTests(unittest.TestCase):
    def test_class_m_base_rows_come_from_orderable_insert_arrangements(self) -> None:
        expected_counts = {
            "1": (INSERTS_8, 48),
            "2": (INSERTS_10, 60),
            "3": (INSERTS_8, 48),
            "4": (INSERTS_10, 60),
        }

        for document_key, (inserts, expected_count) in expected_counts.items():
            with self.subTest(document_key=document_key):
                rows = base_rows_for_extraction(
                    extraction_fixture(document_key, inserts, finishes=CLASS_M_FINISHES)
                )

                self.assertEqual(len(rows), expected_count)

        rows_02 = base_rows_for_extraction(extraction_fixture("2", INSERTS_10, finishes=CLASS_M_FINISHES))
        self.assertEqual(len(rows_02), 60)
        self.assertTrue({"H", "J", "K"}.issubset({row["insert_arrangement_ref"] for row in rows_02}))

    def test_class_p_base_rows_do_not_emit_finish_suffixes(self) -> None:
        for document_key in ["6", "7", "8", "9"]:
            with self.subTest(document_key=document_key):
                rows = base_rows_for_extraction(
                    extraction_fixture(
                        document_key,
                        INSERTS_7,
                        finishes=CLASS_M_FINISHES,
                        components=["insert_arrangement", "wire_type_code", "shell_finish_code"],
                        wire_count=22 if document_key == "8" else 0,
                        class_code="P",
                    )
                )

                self.assertEqual(len(rows), 7)
                self.assertTrue(all(row["shell_finish_code"] is None for row in rows))
                self.assertTrue(all(row["example_full_pin"] and row["example_full_pin"].endswith("01") for row in rows))

    def test_crimp_wire_rows_multiply_from_generated_base_rows(self) -> None:
        crimp_03 = extraction_fixture("3", INSERTS_8, finishes=CLASS_M_FINISHES, components=["insert_arrangement", "wire_type_code", "shell_finish_code"], wire_count=58)
        crimp_04 = extraction_fixture("4", INSERTS_10, finishes=CLASS_M_FINISHES, components=["insert_arrangement", "wire_type_code", "shell_finish_code"], wire_count=58)

        self.assertEqual(len(base_rows_for_extraction(crimp_03)) * len(crimp_03["wire_options"]), 2784)
        self.assertEqual(len(base_rows_for_extraction(crimp_04)) * len(crimp_04["wire_options"]), 3480)

    def test_class_p_crimp_wire_rows_do_not_multiply_by_finish_codes(self) -> None:
        crimp_08 = extraction_fixture(
            "8",
            INSERTS_7,
            finishes=CLASS_M_FINISHES,
            components=["insert_arrangement", "wire_type_code", "shell_finish_code"],
            wire_count=22,
            class_code="P",
        )

        base_rows = base_rows_for_extraction(crimp_08)
        total_wire_rows = sum(len(wire_rows_for_base(crimp_08, f"base-{index}")) for index, _ in enumerate(base_rows))

        self.assertEqual(len(base_rows), 7)
        self.assertEqual(total_wire_rows, 7 * len(crimp_08["wire_options"]))


if __name__ == "__main__":
    unittest.main()
