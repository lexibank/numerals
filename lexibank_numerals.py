import csv
import pathlib

import attr
from clldutils.path import Path
from clldutils.path import walk
from pycldf import Wordlist
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.dataset import Lexeme

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
URL = "http://localhost:8000/cldf.zip"


@attr.s
class NumeralsEntry(Lexeme):
    pass


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
        split_ft = split_form_table(channumerals)
        language_table = list(channumerals["LanguageTable"])

        """
        This splits the list of forms into individual files, grouped by families (or Other for
        smaller families).
        """
        for entry in split_ft:
            lt = next(item for item in language_table if item["ID"] == entry[0]["Language_ID"])

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

            pathlib.Path(self.raw / family).mkdir(parents=True, exist_ok=True)

            # Write data from form table into respective CSV file:
            with open(self.raw / family / csv_name, "w") as outfile:
                fp = csv.DictWriter(outfile, entry[0].keys())
                fp.writeheader()
                fp.writerows(sorted(entry, key=lambda x: int(x["Parameter_ID"])))
                github_file = outfile.name

            # Write index for easier reference:
            with open(index, "a+") as outfile:
                index_link = make_index_link(github_file)
                chan_link = make_chan_link(chansrc, CHANURL)
                language_name = make_language_name(lt["Name"])
                outfile.write(index_link + chan_link + language_name + problems + "\n")

        # Cleanup:
        for f in channnumerals_files:
            Path.unlink(self.raw / f)

    def cmd_install(self, **kw):
        with self.cldf as ds:
            for c in walk(self.raw, mode="files"):
                if c.name == "index.md" or c.name == "README.md":
                    continue
                with Path.open(c) as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        ds.add_language(ID=row["Language_ID"], Glottocode=row["Language_ID"])
                        ds.add_lexemes(
                            Value=row["Value"],
                            Language_ID=row["Language_ID"],
                            Parameter_ID=row["Parameter_ID"],
                        )
