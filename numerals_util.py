from itertools import groupby


def split_form_table(cldf_wordlist):
    """
    Create a list of list of ordered dictionaries, grouped by Language_ID, where each CLDF
    row is mapped to an individual entry.

    Example:  OrderedDict([('ID', 'amar1273-1-1'), ('Parameter_ID', '1'), ...])
    """
    return [
        list(f2)
        for f1, f2 in groupby(
            sorted(cldf_wordlist["FormTable"], key=lambda f1: (f1["Language_ID"])),
            lambda f1: (f1["Language_ID"]),
        )
    ]


def make_index_link(s):
    stripped = s.split("raw/")[-1]
    stripped_link = s.split("raw/")[-1].replace(" ", "%20")
    return f"* [{stripped}]({stripped_link})"


def make_chan_link(s, url):
    s = s.replace(" ", "%20")
    url = url + s
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
