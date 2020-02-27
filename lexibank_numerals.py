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

        valid_parameters = set()
        valid_languages = set()
        for concept in self.concepts:
            args.writer.add_concept(**concept)
            valid_parameters.add(concept['ID'])
        for language in self.languages:
            args.writer.add_language(**language)
            valid_languages.add(language['ID'])

        args.writer.cldf['FormTable', 'Problematic'].datatype.base = 'boolean'

        # gather all overwrite candidates {file_name: path}
        overwrites = {}
        for c in walk(self.dir / "overwrite", mode="files"):
            if c.name == "index.md" or c.name == "README.md"\
                    or c.name in self.channumerals_files\
                    or c.name.startswith("."):
                continue
            overwrites[c.name] = c

        overwrites_cnt = 0
        unknown_params = []
        misaligned_overwrites = set()
        unknown_languages = []
        seen_unknown_languages = set()
        form_length = set()
        other_form = set()

        for c in progressbar(sorted(walk(self.raw_dir, mode="files")), desc="makecldf"):
            if c.name == "index.md" or c.name == "README.md"\
                    or c.name in self.channumerals_files\
                    or c.name.startswith("."):
                continue

            # if an overwrite exists then take the overwrite's path
            if c.name in overwrites:
                c = overwrites[c.name]
                overwrites_cnt += 1

            with Path.open(c) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:

                    if row["Parameter_ID"] not in valid_parameters:
                        unknown_params.append(
                                'Parameter_ID {0} for {1} unknown'.format(
                                        row["Parameter_ID"], row["Language_ID"]))
                        continue

                    if row["Loan"] is None or row["Variant_ID"] is None or\
                                len(row["Loan"]) < 3 or len(row["Variant_ID"]) < 1:
                        misaligned_overwrites.add(row["Language_ID"])

                    if row["Language_ID"] not in valid_languages:
                        if row["Language_ID"] not in seen_unknown_languages:
                            unknown_languages.append({'id': row["Language_ID"], 'lg': c.name})
                            seen_unknown_languages.add(row["Language_ID"])

                    if len(row["Form"]) > len(row["Value"])+1 or\
                            "[" in row["Form"] or "]" in row["Form"]:
                        form_length.add(c.name)

                    if row["Other_Form"] is not None and\
                        ("[" in row["Other_Form"] or "]" in row["Other_Form"]):
                        other_form.add(c.name)

                    args.writer.add_form(
                        Value=row["Value"].strip(),
                        Form=row["Form"].strip(),
                        Language_ID=row["Language_ID"].strip(),
                        Parameter_ID=row["Parameter_ID"].strip(),
                        Source="chan2019",
                        Comment=row["Comment"].strip() if row["Comment"] else "",
                        Other_Form=row["Other_Form"].strip() if row["Other_Form"] else "",
                        Loan=bool(row["Loan"] == "True"),
                        Variant_ID=row["Variant_ID"].strip() if row["Other_Form"] else "",
                        Problematic=bool(row["Problematic"] == "True"),
                    )
        def _x(s):
            try:
                return int(s)
            except:
                return s
        # apply the same sort order as for channumerals
        args.writer.objects['FormTable'] = sorted(args.writer.objects['FormTable'],
                key=lambda item: ([_x(i) for i in item['ID'].split('-')]))

        args.log.info('{0} overwritten languages'.format(overwrites_cnt))

        for u in sorted(unknown_params, key=lambda k: int(k.split(" ")[1])):
            args.log.warn(u)

        for u in sorted(misaligned_overwrites):
            args.log.warn("check overwrite {0} for misalignments".format(u))

        for u in sorted(unknown_languages, key=lambda k: k['lg']):
            args.log.warn("check Language_ID {0} in overwrite {1}".format(u['id'], u['lg']))

        for u in sorted(form_length):
            args.log.warn("check Form in {0}".format(u))

        for u in sorted(other_form):
            args.log.warn("check Other_Form for [] in {0}".format(u))

