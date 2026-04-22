import unittest

from assist.assist_83513_common import latest_base_revision, parse_revision_entries


class AssistRevisionResolutionTests(unittest.TestCase):
    def test_incorporated_amendment_can_be_latest_revision_source(self) -> None:
        page_html = """
        <tr><td><a href="javascript:OpenImage('ImageRedirector.aspx?token=100.1',1);">PDF</a></td>
        <td>Revision H</td><td>A</td><td>18-DEC-2009</td></tr>
        <tr><td><a href="javascript:OpenImage('ImageRedirector.aspx?token=100.2',1);">PDF</a></td>
        <td>Revision H Amendment 4 (all previous amendments incorporated)</td><td>A</td><td>17-DEC-2025</td></tr>
        <tr><td><a href="javascript:OpenImage('ImageRedirector.aspx?token=100.3',1);">PDF</a></td>
        <td>Revision H Notice 1</td><td>A</td><td>18-DEC-2025</td></tr>
        """

        entries = parse_revision_entries(page_html)
        latest = latest_base_revision(entries, expected_revision_letter="H")

        self.assertEqual(latest.image_token, "100.2")
        self.assertEqual(latest.description, "Revision H Amendment 4 (all previous amendments incorporated)")
        self.assertEqual(latest.revision_letter, "H")


if __name__ == "__main__":
    unittest.main()
