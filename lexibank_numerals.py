import csv
import pathlib
from itertools import groupby

from clldutils.path import Path
from pycldf import Wordlist
from pyglottolog import Glottolog
from pylexibank.dataset import Dataset as BaseDataset

CHANURL = "https://mpi-lingweb.shh.mpg.de/numeral/"

# FIXME: Point to Zenodo or GitHub API?
URL = "http://localhost:8000/cldf.zip"

# Largest Glottolog families for sorting:
FAMILIES = [
    "Austronesian",
    "Atlantic-Congo",
    "Sino-Tibetan",
    "Indo-European",
    "Afro-Asiatic",
    "Nuclear Trans New Guinea",
    "Austroasiatic",
    "Tupian",
    "Tai-Kadai",
    "Mande",
    "Pama-Nyungan",
    "Dravidian",
    "Otomanguean",
    "Nilotic",
    "Turkic",
    "Uralic",
    "Central Sudanic",
    "Arawakan",
    "Nakh-Daghestanian",
    "Pano-Tacanan",
    "Uto-Aztecan",
    "Salishan",
    "Algic",
    "Cariban",
]


def make_index_link(s):
    stripped = s.split("raw/")[-1]
    stripped_link = s.split("raw/")[-1].replace(" ", "%20")
    return f"* [{stripped}]({stripped_link})"


def make_chan_link(s):
    s = s.replace(" ", "%20")
    url = CHANURL + s
    return f" ([Source]({url}))"


def check_for_problems(entry):
    for row in entry:
        if row["Problematic"] == "True":
            return " **(Problems)**"

    return ""


def make_language_name(language_name=""):
    if language_name:
        return f" ({language_name})"
    else:
        return ""


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "numerals"

    def cmd_download(self, **kw):
        index = Path(self.raw / "index.md")

        # Create index always from scratch:
        if index.exists():
            Path.unlink(index)

        channnumerals_files = [
            "cldf-metadata.json",
            "cognates.csv",
            "forms.csv",
            "languages.csv",
            "parameters.csv",
        ]

        glottolog = self.glottolog
        languoids = {l.id: l for l in glottolog.languoids()}

        self.raw.download_and_unpack(
            URL, *[Path("cldf", f) for f in channnumerals_files], log=self.log
        )

        channumerals = Wordlist.from_metadata("raw/cldf-metadata.json")

        """
        Create a list of list of ordered dictionaries, grouped by Language_ID, where each CLDF
        row is mapped to an individual entry.

        Example:  OrderedDict([('ID', 'amar1273-1-1'), ('Parameter_ID', '1'), ...])
        """
        split_ft = [
            list(f2)
            for f1, f2 in groupby(
                sorted(channumerals["FormTable"], key=lambda f1: (f1["Language_ID"])),
                lambda f1: (f1["Language_ID"]),
            )
        ]

        """
        This splits the list of forms into individual files, grouped by families (or Other for
        smaller families).
        """
        for entry in split_ft:
            lid = entry[0]["Language_ID"]
            chansrc = entry[0]["SourceFile"]
            family = "Other"
            problems = check_for_problems(entry)
            csv_name = Path(lid + ".csv")

            if languoids[lid].family and languoids[lid].family.name in FAMILIES:
                family = languoids[lid].family.name

            pathlib.Path(self.raw / family).mkdir(parents=True, exist_ok=True)

            with open(self.raw / family / csv_name, "w") as outfile:
                fp = csv.DictWriter(outfile, entry[0].keys())
                fp.writeheader()
                fp.writerows(sorted(entry, key=lambda x: int(x["Parameter_ID"])))
                github_file = outfile.name

            with open(index, "a+") as outfile:
                index_link = make_index_link(github_file)
                chan_link = make_chan_link(chansrc)
                language_name = make_language_name(languoids[lid].name)
                outfile.write(index_link + chan_link + language_name + problems + '\n')

    def cmd_install(self, **kw):
        with self.cldf as _:
            pass
