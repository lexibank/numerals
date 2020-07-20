from pycldf import Wordlist

from lexibank_numerals import CHANURL
from pynumerals.numerals_utils import split_form_table

channumerals = split_form_table(Wordlist.from_metadata("tests/cldf-metadata.json"))


def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_languages(cldf_dataset):
    assert any(l['SourceFile'] == 'Abua.htm' for l in cldf_dataset["LanguageTable"])


def test_forms(cldf_dataset):
    assert any(f["Other_Form"] == "een" for f in cldf_dataset["FormTable"])


class TestUtil:
    @staticmethod
    def test_split_forms():
        """
        This is likely going to break with changes to the upstream data. However, I'd rather see
        this fail in the tests first.
        """
        assert len(channumerals) == 5284
        assert channumerals[0][0]["ID"] == "aari1239-1-1-1"
        assert channumerals[0][0]["Language_ID"] == "aari1239-1"
        assert channumerals[-1][0]["ID"] == "zuoj1238-1-1-1"

    @staticmethod
    def test_index_link():
        from pynumerals.numerals_utils import make_index_link

        assert "* []()" == make_index_link("")
        assert "* [Path/to/file](Path/to/file)" == make_index_link("Path/to/file")
        assert "* [Spaces Spaces](Spaces%20Spaces)" == make_index_link("Spaces Spaces")

    @staticmethod
    def test_chan_link():
        from pynumerals.numerals_utils import make_chan_link

        assert f" ([Source]({CHANURL}))" == make_chan_link("", CHANURL)
        assert f" ([Source]({CHANURL}Bateri.htm))" == make_chan_link("Bateri.htm", CHANURL)

    @staticmethod
    def test_check_problems():
        from pynumerals.numerals_utils import check_for_problems

        assert "" == check_for_problems(channumerals[0])
        assert " **(Problems)**" == check_for_problems(channumerals[3])

    @staticmethod
    def test_language_name():
        from pynumerals.numerals_utils import make_language_name

        assert "" == make_language_name()
        assert " (Sindarin)" == make_language_name("Sindarin")
