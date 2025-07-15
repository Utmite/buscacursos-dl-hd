from itertools import product
import logging
import json
import string
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple

from ..scraper.search import bc_search
from ..scraper.programs import get_program
from ..scraper.requirements import get_requirements
from ..scraper.banner import banner_quota
from .schedule import process_schedule

log = logging.getLogger("scraper")


def _process_and_count_optimized(args):
    comb, period, cfg, processed_nrcs, processed_initials = args

    found = bc_search(cfg, comb, period)
    if cfg.get("testmode", False) and len(found) > 10:
        found = found[:10]

    local_courses = {}
    local_nrcs = set()
    local_initials = set()

    for c in found:
        if c["nrc"] in local_nrcs or c["nrc"] in processed_nrcs:
            continue
        local_nrcs.add(c["nrc"])

        if (
            c["initials"] not in local_initials
            and c["initials"] not in processed_initials
        ):
            program = (
                get_program(cfg, c["initials"]) if cfg.get("fetch-program") else ""
            )
            req, con, restr, equiv = ("", "", "", "")
            if cfg.get("fetch-requirements"):
                req, con, restr, equiv = get_requirements(cfg, c["initials"])

            local_courses[c["initials"]] = {
                "sigle": c["initials"],
                "name": c["name"],
                "credits": c["credits"],
                "req": req,
                "conn": con,
                "restr": restr,
                "equiv": equiv,
                "program": program,
                "school": c["school"],
                "area": c["area"],
                "category": c["category"],
                "sections": {},
            }
            local_initials.add(c["initials"])

        quota = banner_quota(cfg, c["nrc"], period) if cfg.get("fetch-quota") else {}
        sec = {
            "nrc": c["nrc"],
            "section": c["section"],
            "schedule": c["schedule"],
            "format": c["format"],
            "campus": c["campus"],
            "is_english": c["is_english"],
            "is_removable": c["is_removable"],
            "is_special": c["is_special"],
            "category": c["category"],
            "total_quota": c["total_quota"],
            "quota": quota,
        }

        if c["initials"] not in local_courses:
            local_courses[c["initials"]]["sections"] = {}
        local_courses[c["initials"]]["sections"][str(c["section"])] = sec

    return comb, len(found), local_courses, local_nrcs, local_initials


def _merge_results(shared, results_batch, json_path):
    proc_inits = shared["processed_initials"]
    proc_nrcs = shared["processed_nrcs"]
    courses = shared["courses"]
    lock = shared["lock"]

    with lock:
        for _, _, local_courses, local_nrcs, local_initials in results_batch:
            for nrc in local_nrcs:
                proc_nrcs[nrc] = True
            for initial in local_initials:
                proc_inits[initial] = True

            for initial, course_data in local_courses.items():
                if initial in courses:
                    existing_course = courses[initial]
                    existing_course["sections"].update(course_data["sections"])
                    courses[initial] = existing_course
                else:
                    courses[initial] = course_data

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(dict(courses), f, ensure_ascii=False, indent=2)


class CollectCoursesOptimized:
    def __init__(self):
        self.start_time = None

    def _calculate_dynamic_threshold(self, results, depth):
        if not results:
            return 50

        counts = [cnt for _, cnt, *_ in results if cnt > 0]
        if not counts:
            return 50

        avg_count = sum(counts) / len(counts)
        if depth <= 2:
            threshold = max(25, int(avg_count * 0.8))
        elif depth <= 4:
            threshold = max(30, int(avg_count * 1.2))
        else:
            threshold = max(50, int(avg_count * 1.5))

        return min(threshold, 100)

    def _log_progress(self, depth, completed, total, results):
        elapsed = time.time() - self.start_time
        if total > 0:
            progress = (completed / total) * 100
            avg_time = elapsed / completed if completed > 0 else 0
            eta = (total - completed) * avg_time
            total_found = sum(cnt for _, cnt, *_ in results)
            log.info(
                f"Nivel {depth}: {completed}/{total} ({progress:.1f}%) - "
                f"Encontrados: {total_found} - ETA: {eta:.1f}s"
            )

    def collect(self, period: str, cfg: dict):
        json_path: str = f"{period}.json"
        self.start_time = time.time()

        mgr = multiprocessing.Manager()
        shared = {
            "processed_initials": mgr.dict(),
            "processed_nrcs": mgr.dict(),
            "courses": mgr.dict(),
            "lock": mgr.Lock(),
        }

        NUMBERS = string.digits
        MAX_WORKERS = 15  # 1000 segundos sin tener cache

        LETTERS = string.ascii_uppercase
        # Según mis pruebas, hacer [AAA, AAB, AAC,..., ZZX, ZZY, ZZZ] es mejor
        # creo que se debe a que busca cursos tiene un index y la consulta http que es lo que hace que se demore
        # se responden más rápido cuando se hace con prefijos de 3 letras
        #
        # Al buscar AAA vs AA en la web de busca cursos, tambien se nota la diferencia en velocidad (Rocka, 2025-07-01)

        N = 2  # Cantidad de letras en el prefijo inicial, si es 1 es #A, B, ... si es 2 es AA, AB, ..., ZZ, si es 3 es AAA, AAB, ..., ZZZ,
        # NO USAR MAS DE 3 o menos de 1

        prefixes = ["".join(p) for p in product(LETTERS, repeat=N)]

        for depth in range(N, 6):
            if not prefixes:
                break

            log.info(f"Iniciando nivel {depth} con {len(prefixes)} prefijos")
            tasks = [
                (
                    pref,
                    period,
                    cfg,
                    shared["processed_nrcs"],
                    shared["processed_initials"],
                )
                for pref in prefixes
            ]
            results = []
            completed = 0

            with ProcessPoolExecutor(
                max_workers=min(MAX_WORKERS, len(tasks))
            ) as executor:
                future_to_task = {
                    executor.submit(_process_and_count_optimized, task): task
                    for task in tasks
                }
                batch_results = []
                batch_size = 1_000

                for future in as_completed(future_to_task):
                    try:
                        result = future.result()
                        results.append(result)
                        batch_results.append(result)
                        completed += 1

                        if completed % 300 == 0 or completed == len(tasks):
                            self._log_progress(depth, completed, len(tasks), results)

                        if len(batch_results) >= batch_size:
                            _merge_results(shared, batch_results, json_path)
                            batch_results = []

                    except Exception as e:
                        task = future_to_task[future]
                        log.error(f"Error procesando {task[0]}: {e}")
                        completed += 1

                if batch_results:
                    _merge_results(shared, batch_results, json_path)

            threshold = self._calculate_dynamic_threshold(results, depth)
            log.info(f"Aplicando umbral dinámico: {threshold}")

            next_prefixes = []
            pruned_count = 0

            for comb, cnt, *_ in results:
                if cnt < threshold:
                    pruned_count += 1
                    continue

                if depth < 3:
                    next_prefixes.extend([comb + L for L in LETTERS])
                elif depth in (3, 4):
                    next_prefixes.extend([comb + N for N in NUMBERS])

            log.info(
                f"Nivel {depth} completado. Podados: {pruned_count}/{len(results)}. "
                f"Siguientes: {len(next_prefixes)}"
            )
            prefixes = next_prefixes

        total_courses = len(shared["processed_initials"])
        total_sections = len(shared["processed_nrcs"])
        elapsed = time.time() - self.start_time

        log.info("=" * 50)
        log.info("RESUMEN FINAL:")
        log.info(f"Total courses: {total_courses}")
        log.info(f"Total sections: {total_sections}")
        log.info(f"Tiempo total: {elapsed:.2f}s")
        log.info(f"Snapshot final en {json_path}")
        log.info("=" * 50)


class CollectCourses(CollectCoursesOptimized):
    pass
