import csv
import pathlib
import attr
import shutil
import unicodedata
import hashlib

from pyglottolog import Glottolog

from clldutils.path import Path, walk
from pycldf import Wordlist
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.models import Lexeme, Language
from pylexibank.util import progressbar
from pylexibank.forms import FormSpec

from errorcheck import errorchecks
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

    def __attrs_post_init__(self):
        self.Problematic = False
        for check in errorchecks:
            if check(self.Form):
                self.Problematic = True
                break
        if not self.Problematic and self.Other_Form and '<' in self.Other_Form:
            self.Problematic = True


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

        ignored_lang_ids = ["gela1261-3", "hmon1338-1", "scot1243-2", "faro1244-2",
                            "mace1250-2", "nort2627-2", "serb1264-2", "huaa1248-1",
                            "twen1241-2", "brek1238-2", "nang1262-3", "nang1262-4",
                            "guan1266-5", "guan1266-6", "guan1266-7", "orin1239-3",
                            "tase1235-1", "tase1235-2", "whit1267-5", "whit1267-4",
                            "zakh1243-1", "zakh1243-2", "zakh1243-3", "food1238-2",
                            "meta1238-1", "piem1238-2", "piem1238-3", "diga1241-4",
                            "alab1254-2", "yulu1243-1", "yulu1243-2", "caqu1242-2",
                            "inap1243-1", "bayo1255-3", "chuw1238-2", "dalo1238-1",
                            "koma1266-3", "nafa1258-1", "tswa1255-2", "tuni1251-1",
                            "sout2711-1", "zigu1244-1", "kata1264-1", "kata1264-2",
                            "lave1248-2", "adon1237-1", "aust1304-2", "boto1242-4",
                            "inon1237-1", "kuan1248-1", "watu1247-1", "ngaj1237-1",
                            "sout2866-2", "paaf1237-2", "ping1243-2", "farw1235-1",
                            "ravu1237-2", "chha1249-1", "gade1236-2", "paha1251-1",
                            "rohi1238-1", "waig1243-1", "tsak1250-2", "nang1259-2",
                            "bert1249-1", "cogu1240-3", "gamo1244-3", "gamo1244-2",
                            "hrus1242-3", "hupd1244-3", "kair1267-2", "kend1253-1",
                            "komo1258-3", "samo1303-3", "sout3221-1", "tene1248-1",
                            "yora1241-2", "dong1286-2", "rawa1265-6", "tsha1245-1",
                            "jiam1236-12", "jiam1236-15", "araw1273-2", "avac1239-2",
                            "mans1258-3", "mono1275-1"]

        for language in self.languages:

            if language['ID'].strip() in ignored_lang_ids:
                continue

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

        # form IDs and forms which are correct after error checking
        whitelist = {
            "amha1245-1-1-1": "and",
            "amha1245-2-1-1": "and",
            "uppe1438-1-2-1": "notek’a",
            "uppe1438-1-7-1": "donannotek’a",
            "uppe1438-1-12-1": "hilozrunh notek’a mik’ide’",
            "uppe1438-1-22-1": "ts’ełk’inh dina notek’a mik’ide’",
            "uppe1438-1-40-1": "notehina dina",
            "uppe1438-1-50-1": "notehina dina hwlozrunh mik’ide’",
            "uppe1438-1-70-1": "donannotek’a dina",
            "waro1242-2-3-1": "or",
            "west2643-1-4-1": "kõ(o̥/h)mĩ",
            "west2643-1-14-1": "uʃi kõ(o̥/h)mĩ",
            "west2643-1-19-1": "ʃɑʔũ kõ(o̥/h)mĩ",
            "west2643-1-24-1": "oko kõ(o̥/h)mĩ",
            "west2643-1-80-1": "kõ(o̥/h)mĩ ʃiko",
            "west2643-1-90-1": "kõ(o̥/h)mĩ ʃiko uʃi",
        }
        whitelist_datatable_check = [
            "mach1267-1, nant1250-1",
            "bang1353-1, ling1263-1",
            "lano1248-1, sabu1253-1",
            "abai1240-1, selu1243-1",
            "east2472-1, nort2860-1",
            "ouma1237-1, yoba1237-1",
            "sepa1241-1, tere1276-1",
            "arab1268-1, iqui1243-1",
            "sibe1248-1, uisa1238-1",
            "xish1235-2, xxxx0049-1",
            "leal1235-1, soch1239-1",
            "xian1251-2, xian1251-3",
            "yano1261-2, yano1262-2",
            "lada1244-2, lada1244-3",
        ]

        datatable_checks = {}

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

                    if lang_id in ignored_lang_ids:
                        continue
                    if lang_id not in valid_languages:
                        if lang_id not in seen_unknown_languages:
                            unknown_languages.append({'id': lang_id, 'lg': c.name})
                            seen_unknown_languages.add(lang_id)
                        continue

                    param_id = row["Parameter_ID"].strip()
                    if param_id not in valid_parameters:
                        unknown_params.append(
                                'Parameter_ID {0} for {1} unknown'.format(
                                        param_id, lang_id))
                        continue

                    form = unicodedata.normalize('NFC', row["Form"].strip())

                    if form in self.form_spec.missing_data:
                        continue

                    if len(row) != 14:
                        misaligned.add(c)

                    if row["Loan"] is None or\
                            row["Variant_ID"] is None or\
                            len(row["Loan"].strip()) < 3 or\
                            len(row["Variant_ID"].strip()) < 1:
                        misaligned_overwrites.add(lang_id)

                    value = unicodedata.normalize('NFC', row["Value"].strip())

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
                    if lang_id not in datatable_checks:
                        datatable_checks[lang_id] = []
                    datatable_checks[lang_id].append(form)

        # check identical data tables
        for f in datatable_checks:
            datatable_checks[f] = hashlib.md5(
                "".join(datatable_checks[f]).encode("utf-8")).hexdigest()
        datatable_checks_flipped = {}
        for key, value in datatable_checks.items():
            if value not in datatable_checks_flipped:
                datatable_checks_flipped[value] = [key]
            else:
                datatable_checks_flipped[value].append(key)
        for k, v in datatable_checks_flipped.items():
            if len(v) > 1:
                vj = ", ".join(sorted(v))
                if vj not in whitelist_datatable_check:
                    args.log.warn("Check identical data tables in lang_ids: {0}".format(vj))

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

        for u in args.writer.objects['FormTable']:
            if u["Problematic"]:
                if u["ID"] in whitelist and u["Form"] == whitelist[u["ID"]]:
                    u["Problematic"] = False
                else:
                    args.log.warn("{0} -> {1}".format(u["ID"], u["Form"]))

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
