import unittest

from structured_json_validation.build_83513_v2_release import build_release_payload, summarize_payload


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
    wire_count: int = 0,
    class_code: str = "M",
) -> dict:
    unique_cavities = sorted({cavity for _, cavity in inserts})
    slash_sheet = f"{int(document_key):02d}"
    return {
        "source": {
            "spec_sheet": f"MIL-DTL-83513/{int(document_key)}X",
            "document_key": document_key,
            "document_type": "plug_receptacle",
            "title": "Test connector",
            "source_url": "https://example.test",
            "storage_path": f"mil-dtl-83513/{slash_sheet}/test.pdf",
            "revision": "X",
            "source_sha256": f"hash-{slash_sheet}",
            "source_size_bytes": 123,
        },
        "extraction_method": "pdf_first",
        "llm_fallback_required": False,
        "confidence_score": 1.0,
        "issues": [],
        "validation_checks": [],
        "fallback_flags": [],
        "connector_type": "SIGNAL_CONNECTOR",
        "pin_components": {
            "prefix": f"M83513/{slash_sheet}",
            "components": ["insert_arrangement", "wire_type_code", "shell_finish_code"],
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
        "torque_values": [],
        "chunks": [],
    }


class PlatformV2ReleaseTests(unittest.TestCase):
    def test_orderable_arrangements_drive_release_configurations(self) -> None:
        payload = build_release_payload(
            [extraction_fixture("2", INSERTS_10, finishes=CLASS_M_FINISHES)],
            release_name="test-release",
        )

        configs = payload["catalog.configurations"]
        self.assertEqual(len(configs), 60)
        self.assertEqual(
            sorted({row["insert_arrangement_code"] for row in configs}),
            ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
        )
        self.assertEqual(
            sorted({row["shell_finish_code"] for row in configs}),
            ["A", "C", "K", "N", "P", "T"],
        )

    def test_class_p_release_rows_do_not_emit_finish_codes_or_finish_wire_cross_product(self) -> None:
        payload = build_release_payload(
            [
                extraction_fixture(
                    "8",
                    INSERTS_7,
                    finishes=CLASS_M_FINISHES,
                    wire_count=22,
                    class_code="P",
                )
            ],
            release_name="test-release",
        )

        configs = payload["catalog.configurations"]
        self.assertEqual(len(configs), 7)
        self.assertEqual({row["shell_finish_code"] for row in configs}, {None})
        self.assertEqual(len(payload["catalog.wire_options"]), 154)
        self.assertEqual(summarize_payload(payload)["wire_counts_by_slash"], {"08": 154})

    def test_release_payload_contains_active_release_pointer(self) -> None:
        payload = build_release_payload(
            [extraction_fixture("2", INSERTS_10, finishes=CLASS_M_FINISHES)],
            release_name="test-release",
        )

        release = payload["publish.releases"][0]
        active = payload["publish.active_releases"][0]
        self.assertEqual(release["status"], "staged")
        self.assertEqual(active["spec_family"], "83513")
        self.assertEqual(active["release_id"], release["id"])


if __name__ == "__main__":
    unittest.main()
