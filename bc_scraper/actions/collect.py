import logging
import json
import string
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from typing import Dict, List, Union, Tuple
from collections import defaultdict

from ..scraper.search import bc_search
from ..scraper.programs import get_program
from ..scraper.requirements import get_requirements
from ..scraper.banner import banner_quota
from .schedule import process_schedule
from .errors import handle

log = logging.getLogger("scraper")


def _process_and_count_optimized(args):
    """
    Versión optimizada: procesa una combinación y retorna datos locales
    para reducir operaciones costosas en Manager.dict()
    """
    comb, period, cfg = args

    # 1) Buscar
    found = bc_search(cfg, comb, period)
    if cfg.get("testmode", False) and len(found) > 10:
        found = found[:10]

    # 2) Procesar localmente (sin Manager.dict)
    local_courses = {}
    local_nrcs = set()
    local_initials = set()

    for c in found:
        if c["nrc"] in local_nrcs:
            continue

        local_nrcs.add(c["nrc"])
        # log.info(f"Processing: {c['initials']} - {c['section']}")

        # Nuevo curso?
        if c["initials"] not in local_initials:
            program = (
                get_program(cfg, c["initials"]) if cfg.get("fetch-program") else ""
            )
            req, con, restr, equiv = ("", "", "", "")
            if cfg.get("fetch-requirements"):
                req, con, restr, equiv = get_requirements(cfg, c["initials"])

            local_courses[c["initials"]] = {
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

        # Preparar la nueva sección
        quota = banner_quota(cfg, c["nrc"], period) if cfg.get("fetch-quota") else {}
        sec = {
            "nrc": c["nrc"],
            "schedule": process_schedule(c["schedule"]),
            "format": c["format"],
            "campus": c["campus"],
            "is_english": c["is_english"],
            "is_removable": c["is_removable"],
            "is_special": c["is_special"],
            "total_quota": c["total_quota"],
            "quota": quota,
        }

        # Insertar la sección localmente
        if c["initials"] not in local_courses:
            local_courses[c["initials"]]["sections"] = {}
        local_courses[c["initials"]]["sections"][str(c["section"])] = sec

    return comb, len(found), local_courses, local_nrcs, local_initials


def _merge_results(shared, results_batch, json_path):
    """
    Función separada para merge de resultados - reduce contención del lock
    """
    proc_inits = shared["processed_initials"]
    proc_nrcs = shared["processed_nrcs"]
    courses = shared["courses"]
    lock = shared["lock"]

    with lock:
        for _, _, local_courses, local_nrcs, local_initials in results_batch:
            # Merge NRCs y initials
            for nrc in local_nrcs:
                proc_nrcs[nrc] = True
            for initial in local_initials:
                proc_inits[initial] = True

            # Merge courses
            for initial, course_data in local_courses.items():
                if initial in courses:
                    # Merge sections
                    existing_course = courses[initial]
                    existing_course["sections"].update(course_data["sections"])
                    courses[initial] = existing_course
                else:
                    courses[initial] = course_data

        # Escribir snapshot una sola vez por batch
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(dict(courses), f, ensure_ascii=False, indent=2)


class CollectCoursesOptimized:
    def __init__(self):
        self.start_time = None

    def _calculate_dynamic_threshold(self, results, depth):
        """
        Calcula umbral dinámico basado en distribución de resultados
        Más conservador para evitar explorar demasiado
        """
        if not results:
            return 50

        counts = [cnt for _, cnt, *_ in results if cnt > 0]
        if not counts:
            return 50

        # Estadísticas básicas
        avg_count = sum(counts) / len(counts)
        max_count = max(counts)

        # Umbral más conservador: entre 25-100 según profundidad
        if depth <= 2:
            # Niveles iniciales: más permisivo para no perder ramas
            threshold = max(25, int(avg_count * 0.8))
        elif depth <= 4:
            # Niveles medios: más restrictivo
            threshold = max(30, int(avg_count * 1.2))
        else:
            # Niveles profundos: muy restrictivo
            threshold = max(50, int(avg_count * 1.5))

        # Cap máximo para evitar explotar
        return min(threshold, 100)

    def _log_progress(self, depth, completed, total, results):
        """
        Logging mejorado con estadísticas
        """
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

    def collect(self, period: str, cfg: dict, json_path: str = "courses_snapshot.json"):
        """
        Versión optimizada con ProcessPoolExecutor y batching
        """
        self.start_time = time.time()

        mgr = multiprocessing.Manager()
        shared = {
            "processed_initials": mgr.dict(),
            "processed_nrcs": mgr.dict(),
            "courses": mgr.dict(),
            "lock": mgr.Lock(),
        }

        LETTERS = string.ascii_uppercase
        NUMBERS = string.digits
        MAX_WORKERS = min(10, multiprocessing.cpu_count())

        # Nivel 1: solo letras
        prefixes = list(LETTERS)

        for depth in range(1, 6):
            if not prefixes:
                break

            log.info(f"Iniciando nivel {depth} con {len(prefixes)} prefijos")

            # Usar ProcessPoolExecutor para mejor gestión
            tasks = [(pref, period, cfg) for pref in prefixes]
            results = []
            completed = 0

            with ProcessPoolExecutor(
                max_workers=min(MAX_WORKERS, len(tasks))
            ) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(_process_and_count_optimized, task): task
                    for task in tasks
                }

                # Process results as they complete
                batch_results = []
                batch_size = 5  # Procesar en lotes de 5

                for future in as_completed(future_to_task):
                    try:
                        result = future.result()
                        results.append(result)
                        batch_results.append(result)
                        completed += 1

                        # Progress logging
                        if completed % 10 == 0 or completed == len(tasks):
                            self._log_progress(depth, completed, len(tasks), results)

                        # Batch merge para reducir contención
                        if len(batch_results) >= batch_size:
                            _merge_results(shared, batch_results, json_path)
                            batch_results = []

                    except Exception as e:
                        task = future_to_task[future]
                        log.error(f"Error procesando {task[0]}: {e}")
                        completed += 1

                # Merge remaining results
                if batch_results:
                    _merge_results(shared, batch_results, json_path)

            # Preparar siguiente nivel con umbral dinámico
            threshold = self._calculate_dynamic_threshold(results, depth)
            log.info(f"Aplicando umbral dinámico: {threshold}")

            next_prefixes = []
            pruned_count = 0

            for comb, cnt, *_ in results:
                if cnt < threshold:
                    pruned_count += 1
                    continue  # 🔥 PODA: No ramificar si pocos resultados

                # Solo ramificar combinaciones con suficientes resultados
                if depth < 3:
                    # niveles 1–2: añadir letras
                    next_prefixes.extend([comb + L for L in LETTERS])
                elif depth in (3, 4):
                    # nivel 4 y 5: añadir números
                    next_prefixes.extend([comb + N for N in NUMBERS])
                # depth 5: no hay nivel 6 (máximo profundidad)

            log.info(
                f"Nivel {depth} completado. Podados: {pruned_count}/{len(results)}. "
                f"Siguientes: {len(next_prefixes)}"
            )

            prefixes = next_prefixes

        # Resumen final
        total_courses = len(shared["processed_initials"])
        total_sections = len(shared["processed_nrcs"])
        elapsed = time.time() - self.start_time

        log.info("=" * 50)
        log.info(f"RESUMEN FINAL:")
        log.info(f"Total courses: {total_courses}")
        log.info(f"Total sections: {total_sections}")
        log.info(f"Tiempo total: {elapsed:.2f}s")
        log.info(f"Snapshot final en {json_path}")
        log.info("=" * 50)


# Clase legacy para compatibilidad
class CollectCourses(CollectCoursesOptimized):
    """Alias para mantener compatibilidad con código existente"""

    pass
