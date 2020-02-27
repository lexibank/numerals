import csv
import pathlib
import attr
import shutil

from pyglottolog import Glottolog

from clldutils.path import Path, walk
from pycldf import Wordlist
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.models import Lexeme, Language
from pylexibank.util import progressbar
from pylexibank.forms import FormSpec

from numerals_util import (
    split_form_table,
    make_language_name,
    check_for_problems,
    make_index_link,
    make_chan_link,
    FAMILIES,
)

CHANURL = "https://mpi-lingweb.shh.mpg.de/numeral/"

# FIXME: Point to Zenodo or GitHub API?
URL = "https://github.com/lexibank/channumerals/raw/v1.0/cldf"


@attr.s
class NumeralsLanguage(Language):
    SourceFile = attr.ib(default=None)
    Contributor = attr.ib(default=None)
    Base = attr.ib(default=None)
    Comment = attr.ib(default=None)


@attr.s
class NumeralsLexeme(Lexeme):
    Problematic = attr.ib(default=False)
    Other_Form = attr.ib(default=None)
    Variant_ID = attr.ib(default=1)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "numerals"

    lexeme_class = NumeralsLexeme
    language_class = NumeralsLanguage

    form_spec = FormSpec(
        brackets={},
        replacements=[],
        separators="",
        missing_data=(),
        strip_inside_brackets=False,
        normalize_unicode=None,
    )

    channumerals_files = [
        "cldf-metadata.json",
        "cognates.csv",
        "forms.csv",
        "languages.csv",
        "parameters.csv",
        "sources.bib",
    ]

    def cmd_download(self, args):
        glottolog = Glottolog('../glottolog')
        index = Path(self.raw_dir / "index.md")

        # Create index always from scratch:
        if index.exists():
            Path.unlink(index)

        languoids = {l.id: l for l in glottolog.languoids()}

        for f in self.channumerals_files:
            self.raw_dir.download("{0}/{1}".format(URL, f), f, log=args.log)

        channumerals = Wordlist.from_metadata("raw/cldf-metadata.json")
        split_ft = split_form_table(channumerals)
        language_table = list(channumerals["LanguageTable"])

        """
        This splits the list of forms into individual files, grouped by families (or Other for
        smaller families).
        """
        for entry in split_ft:
            lt = next(
                item for item in language_table if item["ID"] == entry[0]["Language_ID"])

            if lt["Glottocode"]:
                lid = lt["Glottocode"]
            else:
                lid = lt["ID"]

            chansrc = lt["SourceFile"]
            problems = check_for_problems(entry)
            csv_name = Path(lt["ID"] + ".csv")
            family = "Other"

            try:
                if languoids[lid].family and languoids[lid].family.name in FAMILIES:
                    family = languoids[lid].family.name
            except KeyError:
                pass

            pathlib.Path(self.raw_dir /
                         family).mkdir(parents=True, exist_ok=True)

            # Write data from form table into respective CSV file:
            with open(self.raw_dir / family / csv_name, "w") as outfile:
                fp = csv.DictWriter(outfile, entry[0].keys())
                fp.writeheader()
                fp.writerows(
                    sorted(entry, key=lambda x: int(x["Parameter_ID"])))
                github_file = outfile.name

            # Write index for easier reference:
            with open(index, "a+") as outfile:
                index_link = make_index_link(github_file)
                chan_link = make_chan_link(chansrc, CHANURL)
                language_name = make_language_name(lt["Name"])
                outfile.write(index_link + chan_link +
                              language_name + problems + "\n")

        shutil.move(self.raw_dir / "cognates.csv",
                    self.etc_dir / "cognates.csv")
        shutil.move(self.raw_dir / "parameters.csv",
                    self.etc_dir / "concepts.csv")
        shutil.move(self.raw_dir / "languages.csv",
                    self.etc_dir / "languages.csv")

    def cmd_makecldf(self, args):

        args.writer.add_sources()

        for concept in self.concepts:
            args.writer.add_concept(**concept)
        for language in self.languages:
            args.writer.add_language(**language)

        args.writer.cldf['FormTable', 'Problematic'].datatype.base = 'boolean'

        for c in progressbar(sorted(walk(self.raw_dir, mode="files"),
                                    key=lambda k: k.name), desc="makecldf"):
            if c.name == "index.md" or c.name == "README.md"\
                    or c.name in self.channumerals_files\
                    or c.name.startswith("."):
                continue
            with Path.open(c) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    args.writer.add_lexemes(
                        Value=row["Value"],
                        Language_ID=row["Language_ID"],
                        Parameter_ID=row["Parameter_ID"],
                        Source="chan2019",
                        Comment=row["Comment"],
                        Other_Form=row["Other_Form"],
                        Loan=bool(row["Loan"] == "True"),
                        Variant_ID=row["Variant_ID"],
                        Problematic=bool(row["Problematic"] == "True"),
                    )
