import csv
import pathlib
from itertools import groupby

from clldutils.path import Path
from pycldf import Wordlist
from pyglottolog import Glottolog
from pylexibank.dataset import Dataset as BaseDataset

# FIXME: Point to Zenodo or GitHub API?
URL = "http://localhost:8000/cldf.zip"

# FIXME: Remove absolute path.
GL = "/home/rzymski@shh.mpg.de/Repositories/glottolog/glottolog"


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "numerals"

    def cmd_download(self, **kw):
        channnumerals_files = [
            "cldf-metadata.json",
            "cognates.csv",
            "forms.csv",
            "languages.csv",
            "parameters.csv",
        ]

        glottolog = Glottolog(GL)
        languoids = {l.id: l for l in glottolog.languoids()}

        self.raw.download_and_unpack(
            URL, *[Path("cldf", f) for f in channnumerals_files], log=self.log
        )

        channumerals = Wordlist.from_metadata("raw/cldf-metadata.json")
        channumerals_forms = list(channumerals["FormTable"])

        split_ft = [
            list(f2)
            for f1, f2 in groupby(
                sorted(channumerals_forms, key=lambda f1: (f1["Language_ID"])),
                lambda f1: (f1["Language_ID"]),
            )
        ]

        for entry in split_ft:
            lid = entry[0]["Language_ID"]
            mc = "None"

            if languoids[lid].macroareas:
                mc = languoids[lid].macroareas[0].name

            pathlib.Path("raw/" + mc).mkdir(parents=True, exist_ok=True)

            with open("raw/" + mc + "/" + lid, "w") as outfile:
                fp = csv.DictWriter(outfile, entry[0].keys())
                fp.writeheader()
                fp.writerows(sorted(entry, key=lambda x: int(x["Parameter_ID"])))

    def cmd_install(self, **kw):
        with self.cldf as _:
            pass
