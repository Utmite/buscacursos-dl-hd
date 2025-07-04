from html.parser import HTMLParser
from time import sleep

from .request import get_text
import logging
from typing import List, Dict, Tuple, Union, Optional

log = logging.getLogger("scraper")


from typing import Dict, List


def process_schedule(text_sc: str) -> Dict[str, List[str]]:
    """For a given schedule text in BC format, returns the SQL queries for inserting
    the full schedule and schedule info. Those queries have to format ID.
    """
    # Full Schedule
    data = text_sc.split("\nROW: ")[1:]
    # data rows -> day-day:module,module <> type <> room <><>
    schedule: Dict[str, List[str]] = {}
    for row in data:
        row = row.split("<>")
        while len(row) > 0 and row[-1] == "":
            row.pop()
        horario = row[0].split(":")
        days = horario[0].split("-")
        modules = horario[1].split(",")
        for day in days:
            for mod in modules:
                if len(day) and len(mod):
                    schedule[day.lower() + mod] = row[1:]
    return schedule


class _BCParser(HTMLParser):
    toogle: bool
    nested: int
    text: str
    current_school: str
    courses: List[Dict[str, Union[str, bool, int]]]

    def __init__(self):
        super().__init__()
        self.toogle = False
        self.nested = 0
        self.text = ""
        self.current_school = ""
        self.courses = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        if tag == "tr" and (
            ("class", "resultadosRowPar") in attrs
            or ("class", "resultadosRowImpar") in attrs
        ):
            self.toogle = True
        elif tag == "tr" and self.toogle:
            self.nested += 1
            self.text += f"<{tag}>"
        elif self.toogle:
            self.text += f"<{tag}>"

        if tag == "td" and ("colspan", "18") in attrs:
            self.current_school = "*"

    def handle_endtag(self, tag: str):
        if tag == "tr" and self.toogle:
            if self.nested:
                self.nested -= 1
            else:
                self.toogle = False
                self.process_course()
                self.text = ""
        elif self.toogle:
            self.text += f"</{tag}>"
            if tag == "td":
                self.text += "\n"

    def handle_data(self, data: str):
        if self.toogle:
            data = data.strip()
            self.text += data

        if self.current_school == "*":
            self.current_school = data

    def process_course(self):
        data = self.text.strip().split("\n")
        for index in range(len(data)):
            # data[index] = data[index][4:-5] # strip <td> </td>
            data[index] = data[index].replace("<td>", "").replace("</td>", "")
            data[index] = data[index].replace("<br>", "").replace("</br>", "")

        course = {
            "nrc": data[0],
            "initials": data[1][data[1].index("</img>") + 6 : data[1].index("</div>")],
            "is_removable": False if data[2] == "NO" else True,
            "is_english": False if data[3] == "NO" else True,
            "section": int(data[4]),
            "is_special": False if data[5] == "NO" else True,
            "area": data[6],
            "format": data[7][:16],
            "category": data[8],
            "name": data[9],
            "teachers": data[10].replace("<a>", "").replace("</a>", ""),
            "campus": data[11],
            "credits": int(data[12]),
            "total_quota": int(data[13]),
            "available_quota": int(data[14]),
            "schedule": "<>".join(data[16:]).replace("<tr>", "\nROW: "),
            "school": self.current_school,
        }
        # Quick horario processing
        course["schedule"] = course["schedule"].replace("<a>", "").replace("</a>", "")
        course["schedule"] = (
            course["schedule"].replace("<table>", "").replace("</table>", "")
        )
        course["schedule"] = (
            course["schedule"].replace("<img>", "").replace("</img>", "")
        )

        course["schedule"] = process_schedule(course["schedule"])

        # Turn profesor Apellido Nombre to Nombre Apellido
        if course["teachers"] not in [
            "Dirección Docente",
            "(Sin Profesores)",
            "Por Fijar",
        ]:
            course["teachers"] = ",".join(
                [
                    prof.split(" ")[-1] + " " + " ".join(prof.split(" ")[:-1])
                    for prof in course["teachers"].split(",")
                ]
            )

        self.courses.append(course)


# Search
def bc_search(cfg, query: str, period: str, nrc: bool = False):
    parser = _BCParser()
    url = None
    if nrc:
        url = f"https://buscacursos.uc.cl/?cxml_semestre={period}&cxml_nrc={query}"
    else:
        url = f"https://buscacursos.uc.cl/?cxml_semestre={period}&cxml_sigla={query}"
    resp = get_text(cfg, url)

    # Check valid response
    if len(resp) < 1000:
        log.warn("Too many request prevention")
        sleep(5)
        resp = get_text(cfg, url)

    parser.feed(resp)
    return parser.courses
