import unittest

from hybrid_extraction.m83513_extraction_engine import (
    ExtractionResult,
    ExtractionSource,
    build_validation_checks,
    parse_pcb_configuration_rows,
    parse_pin_components,
)
from hybrid_extraction.m83513_extraction_registry import document_type_for_key


class PcbConfigurationParserTests(unittest.TestCase):
    def test_accepts_three_four_and_five_column_pcb_tables(self) -> None:
        three_column_page = (
            "MIL-DTL-83513/28B Number A B of Max .005 D contacts "
            "9 .785 .565 .3338 (19.94) (14.35) (8.478) "
            "15 .935 .715 .4838 (23.75) (18.16) (12.289)"
        )
        three_column_rows = parse_pcb_configuration_rows([three_column_page], {9, 15})

        self.assertEqual(len(three_column_rows), 2)
        self.assertEqual(three_column_rows[0]["dimensions"]["A"], 0.785)
        self.assertEqual(three_column_rows[0]["dimensions"]["B"], 0.565)
        self.assertEqual(three_column_rows[0]["dimensions"]["D"], 0.3338)
        self.assertNotIn("C", three_column_rows[0]["dimensions"])

        four_column_page = (
            "MIL-DTL-83513/19D Number C .005 of A max B .005 D min contacts "
            "9 1.390 1.150 .565 .3342 (35.31) (29.21) (14.35) (8.489)"
        )
        four_column_rows = parse_pcb_configuration_rows([four_column_page], {9})

        self.assertEqual(four_column_rows[0]["dimensions"]["A"], 1.390)
        self.assertEqual(four_column_rows[0]["dimensions"]["B"], 1.150)
        self.assertEqual(four_column_rows[0]["dimensions"]["C"], 0.565)
        self.assertEqual(four_column_rows[0]["dimensions"]["D"], 0.3342)

        split_header_page = "\n".join(
            [
                "MIL-DTL-83513/19D",
                "Number C .005",
                "of A max B .005 D min",
                "contacts",
                "9 1.390 1.150 .565 .3342",
            ]
        )
        split_header_rows = parse_pcb_configuration_rows([split_header_page], {9})

        self.assertEqual(split_header_rows[0]["dimensions"]["A"], 1.390)
        self.assertEqual(split_header_rows[0]["dimensions"]["D"], 0.3342)

        five_column_page = (
            "MIL-DTL-83513/22D Number A B C E of Max .007 .005 D Max contacts "
            "9 1.390 1.150 .565 .3338 .885 (35.31) (29.21) (14.35) (8.478) (22.48)"
        )
        five_column_rows = parse_pcb_configuration_rows([five_column_page], {9})

        self.assertEqual(five_column_rows[0]["dimensions"]["A"], 1.390)
        self.assertEqual(five_column_rows[0]["dimensions"]["B"], 1.150)
        self.assertEqual(five_column_rows[0]["dimensions"]["C"], 0.565)
        self.assertEqual(five_column_rows[0]["dimensions"]["D"], 0.3338)
        self.assertEqual(five_column_rows[0]["dimensions"]["E"], 0.885)

    def test_distinguishes_seven_column_pcb_label_sets(self) -> None:
        h_label_page = (
            "MIL-DTL-83513/10D Number of A B D E F G H contacts "
            "9 .787 .565 .3338 .425 .425 .230 .787 (19.99) (14.35)"
        )
        h_label_rows = parse_pcb_configuration_rows([h_label_page], {9})

        self.assertEqual(h_label_rows[0]["dimensions"]["D"], 0.3338)
        self.assertEqual(h_label_rows[0]["dimensions"]["H"], 0.787)
        self.assertNotIn("C", h_label_rows[0]["dimensions"])

        c_label_page = (
            "MIL-DTL-83513/16D Number B C of A .007 .005 D E F G contacts Max "
            "9 1.390 1.150 .565 .3338 .1848 .465 .325 (35.31) (29.21)"
        )
        c_label_rows = parse_pcb_configuration_rows([c_label_page], {9})

        self.assertEqual(c_label_rows[0]["dimensions"]["C"], 0.565)
        self.assertEqual(c_label_rows[0]["dimensions"]["D"], 0.3338)
        self.assertEqual(c_label_rows[0]["dimensions"]["G"], 0.325)
        self.assertNotIn("H", c_label_rows[0]["dimensions"])

    def test_empty_pcb_dimensions_are_reported_as_validation_flag(self) -> None:
        source = ExtractionSource(
            spec_sheet="MIL-DTL-83513/11D",
            document_key="11",
            document_type="pcb_tail",
            title="PCB tail test sheet",
            source_url="https://example.test",
            storage_path="mil-dtl-83513/11/test.pdf",
            revision="D",
        )
        result = ExtractionResult(
            source=source,
            connector_type="PCB_TAIL_CONNECTOR",
            cavity_counts=[51],
            pin_components={"insert_arrangements": [{"insert_arrangement": "G", "cavity_count": 51}]},
            configuration_rows=[
                {
                    "page_number": 1,
                    "cavity_count": 51,
                    "shell_size_letter": "B",
                    "dimensions": {},
                }
            ],
        )

        _, fallback_flags = build_validation_checks(document_type_for_key("11"), result)

        self.assertIn("empty_configuration_dimensions", fallback_flags)


class PinComponentParserTests(unittest.TestCase):
    def test_02h_pin_block_preserves_h_j_k_orderable_arrangements(self) -> None:
        page = (
            "Part or Identifying Number (PIN): PIN shall consist of the letter M, "
            "the basic number of the specification sheet, a letter from the insert column "
            "and the shell finish. M83513/02 - A C Specification sheet Insert arrangement "
            "Shell finish (see figure 2) (Interface critical) A = 9 A = Pure electrodeposited aluminum "
            "B = 15 c = Cadmium C = 21 K = Zinc nickel D = 25 N = electroless nickel "
            "E = 31 (space applications only) F = 37 P = Passivated Stainless Steel "
            "G = 51 T = Nickel Fluorocarbon Polymer H = 100 (See NOTE) "
            "J = 100 (See NOTE) K = 100 (See NOTE) NOTE: Insert arrangement H is configuration C."
        )

        components = parse_pin_components([page], "2", "plug_receptacle")

        self.assertEqual(
            [item["insert_arrangement"] for item in components["insert_arrangements"]],
            ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
        )
        self.assertEqual(
            [item["code"] for item in components["shell_finish_options"]],
            ["A", "C", "K", "N", "P", "T"],
        )

    def test_04k_pin_block_preserves_h_j_k_orderable_arrangements(self) -> None:
        page = (
            "Part or Identifying Number (PIN): Consists of the letter M, the basic number "
            "of the specification sheet, a letter from the insert column and the shell finish. "
            "M83513/04 - A 01 C Specification sheet Insert Wire Shell finish number arrangement "
            "type (Interface critical) (see figure 2) A = 9 A = Pure electrodeposited "
            "B = 15 aluminum C = 21 C = Cadmium D = 25 K = Zinc nickel E = 31 "
            "N = electroless nickel F = 37 P = Passivated stainless steel G = 51 "
            "T = Nickel fluorocarbon polymer H = 100 /10 J = 100 /11 K = 100 /12"
        )

        components = parse_pin_components([page], "4", "plug_receptacle")

        self.assertEqual(
            [item["insert_arrangement"] for item in components["insert_arrangements"]],
            ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
        )

    def test_07e_class_p_has_no_finish_suffixes(self) -> None:
        page = (
            "Part or Identifying Number (PIN): M83513/07 - A Specification sheet "
            "Insert arrangement A = 9 B = 15 C = 21 D = 25 E = 31 F = 37 G = 51 "
            "A = Pure electrodeposited aluminum C = Cadmium"
        )

        components = parse_pin_components([page], "7", "plug_receptacle")

        self.assertEqual(
            [item["insert_arrangement"] for item in components["insert_arrangements"]],
            ["A", "B", "C", "D", "E", "F", "G"],
        )
        self.assertEqual(components["shell_finish_options"], [])
        self.assertEqual(components["format_example"], "M83513/07-A")

    def test_02_missing_j_k_is_a_validation_failure(self) -> None:
        source = ExtractionSource(
            spec_sheet="MIL-DTL-83513/2H",
            document_key="2",
            document_type="plug_receptacle",
            title="Class M solder receptacle",
            source_url="https://example.test",
            storage_path="mil-dtl-83513/02/test.pdf",
            revision="H",
        )
        result = ExtractionResult(
            source=source,
            connector_type="SIGNAL_CONNECTOR",
            cavity_counts=[9, 15, 21, 25, 31, 37, 51, 100],
            pin_components={
                "insert_arrangements": [
                    {"insert_arrangement": code, "cavity_count": cavity}
                    for code, cavity in zip(
                        ["A", "B", "C", "D", "E", "F", "G", "H"],
                        [9, 15, 21, 25, 31, 37, 51, 100],
                        strict=False,
                    )
                ],
                "shell_finish_options": [
                    {"code": code, "description": code}
                    for code in ["A", "C", "K", "N", "P", "T"]
                ],
            },
            configuration_rows=[{"page_number": 1, "cavity_count": 100, "dimensions": {"unit": "inch"}}],
        )

        _, fallback_flags = build_validation_checks(document_type_for_key("2"), result)

        self.assertIn("unexpected_insert_arrangements", fallback_flags)


if __name__ == "__main__":
    unittest.main()
