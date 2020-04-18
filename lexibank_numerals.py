import csv
import pathlib
import attr
import shutil
import unicodedata

from pyglottolog import Glottolog

from clldutils.path import Path, walk
from pycldf import Wordlist
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.models import Lexeme, Language
from pylexibank.util import progressbar
from pylexibank.forms import FormSpec

from base_mapper import BASE_MAP

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
class CustomLanguage(Language):
    SourceFile = attr.ib(default=None)
    Contributor = attr.ib(default=None)
    Base = attr.ib(default=None)
    Comment = attr.ib(default=None)


@attr.s
class CustomLexeme(Lexeme):
    Problematic = attr.ib(default=False)
    Other_Form = attr.ib(default=None)
    Variant_ID = attr.ib(default=1)


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "numerals"

    lexeme_class = CustomLexeme
    language_class = CustomLanguage

    form_spec = FormSpec(
        brackets={},
        replacements=[],
        separators="",
        missing_data=["Ø"],
        strip_inside_brackets=False,
        normalize_unicode="NFC",
    )

    channumerals_files = [
        "cldf-metadata.json",
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

        shutil.move(self.raw_dir / "parameters.csv",
                    self.etc_dir / "concepts.csv")
        shutil.move(self.raw_dir / "languages.csv",
                    self.etc_dir / "languages.csv")

    def cmd_makecldf(self, args):

        args.writer.add_sources()

        valid_parameters = set()
        valid_languages = set()
        changed_glottolog_codes = []
        no_glottolog_codes = []

        for concept in self.concepts:
            args.writer.add_concept(**concept)
            valid_parameters.add(concept['ID'])
        for language in self.languages:
            if language["Base"]:
                if language["Base"] in BASE_MAP:
                    language["Base"] = BASE_MAP[language["Base"]]
                else:
                    args.log.warn("Base '{0}' is known for {1}".format(
                        language["Base"], language['ID']))
            args.writer.add_language(**language)
            valid_languages.add(language['ID'])
            if language['Glottocode']:
                if language['ID'].split("-")[0] != language['Glottocode']:
                    changed_glottolog_codes.append(
                        (language['ID'], language['Glottocode'])
                    )
            else:
                no_glottolog_codes.append(language['ID'])


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
        misaligned = set()
        unknown_languages = []
        seen_unknown_languages = set()
        form_length = set()
        other_form = set()

        # only for avoiding outputting a warning
        ignored_lang_ids = ["gela1261-3", "hmon1338-1", "scot1243-2", "faro1244-2",
                            "mace1250-2", "nort2627-2", "serb1264-2", "huaa1248-1",
                            "twen1241-2", "brek1238-2", "nang1262-3", "nang1262-4",
                            "guan1266-5", "guan1266-6", "guan1266-7", "orin1239-3",
                            "tase1235-1", "tase1235-2", "whit1267-5", "whit1267-4",
                            "zakh1243-1", "zakh1243-2", "zakh1243-3", "food1238-2",
                            "meta1238-1", "piem1238-2", "piem1238-3", "diga1241-4"]

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

                    lang_id = row["Language_ID"].strip()
                    param_id = row["Parameter_ID"].strip()
                    form = unicodedata.normalize('NFC', row["Form"].strip())
                    value = unicodedata.normalize('NFC', row["Value"].strip())

                    if lang_id not in valid_languages:
                        if lang_id not in ignored_lang_ids\
                                and lang_id not in seen_unknown_languages:
                            unknown_languages.append({'id': lang_id, 'lg': c.name})
                            seen_unknown_languages.add(lang_id)
                        continue

                    if param_id not in valid_parameters:
                        unknown_params.append(
                                'Parameter_ID {0} for {1} unknown'.format(
                                        param_id, lang_id))
                        continue

                    if form in self.form_spec.missing_data:
                        continue

                    if len(row) != 14:
                        misaligned.add(c)

                    if row["Loan"] is None or\
                            row["Variant_ID"] is None or\
                            len(row["Loan"].strip()) < 3 or\
                            len(row["Variant_ID"].strip()) < 1:
                        misaligned_overwrites.add(lang_id)

                    if len(form) > len(value)+1 or\
                            "[" in form or "]" in form:
                        form_length.add(c.name)

                    if row["Other_Form"] is not None and\
                            ("[" in row["Other_Form"] or "]" in row["Other_Form"]):
                        other_form.add(c.name)

                    args.writer.add_form(
                        Value=value,
                        Form=form,
                        Language_ID=lang_id,
                        Parameter_ID=param_id,
                        Source="chan2019",
                        Comment=row["Comment"].strip() if row["Comment"].strip() else "",
                        Other_Form=row["Other_Form"].strip() if row["Other_Form"].strip() else "",
                        Loan=bool(row["Loan"].strip() == "True"),
                        Variant_ID=row["Variant_ID"].strip() if row["Variant_ID"].strip() else "",
                        Problematic=bool(row["Problematic"].strip() == "True"),
                    )

        def _x(s):
            try:
                return int(s)
            except ValueError:
                return s

        # apply the same sort order as for channumerals
        args.writer.objects['FormTable'] = sorted(
                args.writer.objects['FormTable'],
                key=lambda item: ([_x(i) for i in item['ID'].split('-')])
            )

        args.log.info('{0} overwritten languages'.format(overwrites_cnt))

        for u in changed_glottolog_codes:
            args.log.info("changed {0} to {1}".format(u[0], u[1]))

        for u in ignored_lang_ids:
            args.log.info("removed ID {0}".format(u))

        for u in no_glottolog_codes:
            args.log.info("no Glottolog code for ID {0}".format(u))

        for u in sorted(unknown_params, key=lambda k: int(k.split(" ")[1])):
            args.log.warn(u)

        for u in sorted(misaligned_overwrites):
            args.log.warn("check overwrite {0} for misalignments".format(u))

        for u in sorted(misaligned):
            args.log.warn("check {0} for number of colums".format(u))

        for u in sorted(unknown_languages, key=lambda k: k['lg']):
            args.log.warn("check Language_ID {0} in overwrite {1}".format(u['id'], u['lg']))

        for u in sorted(form_length):
            args.log.warn("check Form in {0}".format(u))

        for u in sorted(other_form):
            args.log.warn("check Other_Form for [] in {0}".format(u))
