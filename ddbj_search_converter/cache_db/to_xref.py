import re
from typing import Dict, Pattern

from ddbj_search_converter.schema import Xref, XrefType

ID_PATTERN_MAP: Dict[XrefType, Pattern[str]] = {
    "biosample": re.compile(r"^SAM[NED](\w)?\d+$"),
    "bioproject": re.compile(r"^PRJ[DEN][A-Z]\d+$"),
    "sra-experiment": re.compile(r"[SDE]RX\d+"),
    "sra-run": re.compile(r"[SDE]RR\d+"),
    "sra-sample": re.compile(r"[SDE]RS\d+"),
    "sra-study": re.compile(r"[SDE]RP\d+"),
    "gea": re.compile(r"^E-GEAD-\d+$"),
    "insdc-assembly": re.compile(r"^GCA_[0-9]{9}(\.[0-9]+)?$"),
    "insdc-master": re.compile(r"^([A-Z]0{5}|[A-Z]{2}0{6}|[A-Z]{4,6}0{8,10}|[A-J][A-Z]{2}0{5})$"),
    "insdc": re.compile(r"^([A-Z]\d{5}|[A-Z]{2}\d{6}|[A-Z]{4,6}\d{8,10}|[A-J][A-Z]{2}\d{5})(\.\d+)?$"),
    "metabobank": re.compile(r"^MTB"),
    "taxonomy": re.compile(r"^\d+"),
}

URL_TEMPLATE: Dict[XrefType, str] = {
    "biosample": "https://ddbj.nig.ac.jp/resource/biosample/{id}",
    "bioproject": "https://ddbj.nig.ac.jp/resource/bioproject/{id}",
    "sra-experiment": "https://ddbj.nig.ac.jp/resource/sra-experiment/{id}",
    "sra-run": "https://ddbj.nig.ac.jp/resource/sra-run/{id}",
    "sra-sample": "https://ddbj.nig.ac.jp/resource/sra-sample/{id}",
    "sra-study": "https://ddbj.nig.ac.jp/resource/sra-study/{id}",
    "gea": "https://ddbj.nig.ac.jp/public/ddbj_database/gea/experiment/{prefix}/{id}/",
    "insdc-assembly": "https://www.ncbi.nlm.nih.gov/datasets/genome/{id}",
    "insdc-master": "https://www.ncbi.nlm.nih.gov/nuccore/{id}",
    "insdc": "https://getentry.ddbj.nig.ac.jp/getentry?database=ddbj&accession_number={id}",
    "metabobank": "https://mb2.ddbj.nig.ac.jp/study/{id}.html",
    "taxonomy": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id={id}",
}


def to_xref(id_: str) -> Xref:
    for db_type, pattern in ID_PATTERN_MAP.items():
        if pattern.match(id_):
            url_template = URL_TEMPLATE[db_type]
            if db_type == "gea":
                gea_id_num = int(id_.lstrip("E-GEA-"))
                prefix = f"E-GEAD-{(gea_id_num // 1000) * 1000:03d}"
                url = url_template.format(prefix=prefix, id=id_)
            else:
                url = url_template.format(id=id_)

            return Xref(identifier=id_, type=db_type, url=url)

    # default は taxonomy を返す
    return Xref(identifier=id_, type="taxonomy", url=URL_TEMPLATE["taxonomy"].format(id=id_))
