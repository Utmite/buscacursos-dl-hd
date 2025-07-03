import logging
from time import sleep
from typing import List, Dict, Union
from lxml import html

from .request import get_text

log = logging.getLogger("scraper")


def bc_search(
    cfg, query: str, period: str, nrc: bool = False
) -> List[Dict[str, Union[str, bool, int]]]:
    """
    Parser rápido usando lxml para extraer resultados de busacursos.uc.cl
    """
    # Construir URL
    if nrc:
        url = f"https://buscacursos.uc.cl/?cxml_semestre={period}&cxml_nrc={query}"
    else:
        url = f"https://buscacursos.uc.cl/?cxml_semestre={period}&cxml_sigla={query}"

    resp_text = get_text(cfg, url)
    if len(resp_text) < 1000:
        log.warning("Respuesta muy corta: posible bloqueo. Reintentando...")
        sleep(5)
        resp_text = get_text(cfg, url)

    # Parsear con lxml
    tree = html.fromstring(resp_text)
    # Buscar todas las filas de resultados
    rows = tree.xpath(
        '//tr[contains(@class, "resultadosRowPar") or contains(@class, "resultadosRowImpar")]'
    )

    courses: List[Dict[str, Union[str, bool, int]]] = []
    current_school: str = ""

    for row in rows:
        # Detectar encabezado de escuela
        if row.xpath('.//td[@colspan="18"]'):
            school_name = row.text_content().strip()
            current_school = school_name
            continue

        cells = [td.text_content().strip() for td in row.xpath(".//td")]
        if len(cells) < 15:
            continue

        # Extraer datos básicos
        nrc = cells[0]
        initials_block = cells[1]
        # Si hay etiqueta </img> y </div>
        if "</img>" in initials_block and "</div>" in initials_block:
            initials = initials_block.split("</img>")[-1].split("</div>")[0]
        else:
            initials = initials_block

        course: Dict[str, Union[str, bool, int]] = {
            "nrc": nrc,
            "initials": initials,
            "is_removable": cells[2] != "NO",
            "is_english": cells[3] != "NO",
            "section": int(cells[4] or 0),
            "is_special": cells[5] != "NO",
            "area": cells[6],
            "format": cells[7][:16],
            "category": cells[8],
            "name": cells[9],
            "teachers": cells[10],
            "campus": cells[11],
            "credits": int(cells[12] or 0),
            "total_quota": int(cells[13] or 0),
            "available_quota": int(cells[14] or 0),
            "schedule": "",
            "school": current_school,
        }

        # Procesar horario si existen más celdas
        if len(cells) > 16:
            # Reconstruir HTML de celdas horario
            sched_elements = row.xpath(".//td[position() > 16]")
            sched_texts = []
            for cell in sched_elements:
                # eliminar tags internos y obtener texto
                sched_texts.append(cell.text_content().strip())
            course["schedule"] = "\nROW: ".join(sched_texts)

        # Reordenar nombres de profesores Apellido Nombre -> Nombre Apellido
        bad_names = {"Dirección Docente", "(Sin Profesores)", "Por Fijar"}
        if course["teachers"] not in bad_names:
            profs = [p for p in course["teachers"].split(",") if p]
            reordered = []
            for prof in profs:
                parts = prof.split()
                if len(parts) > 1:
                    reordered.append(" ".join(parts[1:] + parts[:1]))
                else:
                    reordered.append(prof)
            course["teachers"] = ",".join(reordered)

        courses.append(course)

    return courses
