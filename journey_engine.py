import os
import json
import re
import datetime
import urllib.request
import urllib.error
from collections import defaultdict
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Human-readable stage definitions
CANONICAL_STAGES = [
    "App launched",
    "Language screen",
    "Language download chosen",
    "Category screen",
    "Category download pressed",
    "Onboarding carousel",
    "T&C accepted",
    "Survey started",
    "Survey completed",
    "Onboarding completed",
    "Home screen",
]

SCREEN_LABELS = {
    "splash_screen": "Splash",
    "language_list_screen": "Language list",
    "category_list_screen": "Category list",
    "onboarding_screen": "Onboarding carousel",
    "settings_disclaimer_screen": "Disclaimer",
    "onboarding_survey_screen": "Onboarding survey",
    "onboarding_complete_screen": "Onboarding complete",
    "home_screen": "Home",
    "modules_screen": "Modules",
    "search_screen": "Search",
    "my_learning_screen": "My learning",
    "user_profile_screen": "User profile",
    "notification_screen": "Notifications",
    "settings_feedback_screen": "Settings feedback",
    "leader_screen": "Leader screen",
    "my_learning_landing_screen": "My learning landing",
    "category_modules_screen": "Category modules",
    "module_detail_screen": "Module detail",
    "chapter_detail_screen": "Chapter detail",
    "action_card_chapters_screen": "Action card chapters",
    "drug_list_screen": "Drug list",
    "quiz_completion_screen": "Quiz completion",
    "quiz_screen": "Quiz",
}

PHASES = {
    "splash": "Splash",
    "language": "Language",
    "category": "Categories",
    "onboarding": "Onboarding",
    "survey": "Survey",
    "app": "In app (Home+)",
}


def get_screen_phase(screen_name: str) -> str:
    if screen_name in ("splash_screen",):
        return "splash"
    if screen_name in ("language_list_screen",):
        return "language"
    if screen_name in ("category_list_screen",):
        return "category"
    if screen_name in ("onboarding_screen", "settings_disclaimer_screen"):
        return "onboarding"
    if screen_name in ("onboarding_survey_screen", "onboarding_complete_screen"):
        return "survey"
    return "app"


def format_human_duration(sec):
    if sec is None or pd.isna(sec):
        return "—"
    sec = float(sec)
    if sec < 60:
        return f"{sec:.1f}s"
    m = int(sec // 60)
    s = int(sec % 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h = int(m // 60)
    m_rem = m % 60
    if h < 24:
        return f"{h}h {m_rem:02d}m"
    d = int(h // 24)
    return f"{d}d {h % 24}h"


class UserJourneyEngine:
    # In-memory cache: (pattern_key) -> {"finding": ..., "fix": ...}
    _issue_ai_cache: dict = {}

    def __init__(self, pg_kwargs=None):
        self.pg_kwargs = pg_kwargs or dict(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
            user=os.getenv("POSTGRES_USER", "vanna_readonly"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    # ------------------------------------------------------------------
    # AI-powered dynamic issue analysis via Qwen (Ollama)
    # ------------------------------------------------------------------
    def _ai_enhance_issue(self, area: str, facts: dict, fallback_finding: str, fallback_fix: str) -> dict:
        """Call Qwen via Ollama /api/chat to generate a concise finding + fix recommendation
        for a detected issue pattern. Falls back to static text if Ollama is
        unavailable or times out.

        Uses /api/chat (not /api/generate) with think=False because qwen3 models
        run in thinking mode by default and return an empty 'response' field
        via /api/generate.
        """
        # Build a stable cache key from area + sorted facts
        cache_key = area + "|" + json.dumps(facts, sort_keys=True)
        if cache_key in UserJourneyEngine._issue_ai_cache:
            return UserJourneyEngine._issue_ai_cache[cache_key]

        ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
        model = os.getenv("OLLAMA_MODEL", "qwen3:8b")

        facts_str = "\n".join(f"  - {k}: {v}" for k, v in facts.items())
        user_msg = (
            f"Issue: {area}\n"
            f"Facts:\n{facts_str}\n\n"
            f"Write exactly two lines (no markdown, no extra text):\n"
            f"FINDING: <one sentence, max 25 words, describing what is happening>\n"
            f"FIX: <one sentence, max 25 words, actionable recommendation for the dev team>"
        )

        payload = json.dumps({
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a mobile-app analytics expert for Maternity Foundation's "
                        "'Safe Delivery' onboarding app. Always respond in the exact two-line "
                        "format: FINDING: ... then FIX: ... with no additional text."
                    ),
                },
                {"role": "user", "content": user_msg},
            ],
            "stream": False,
            "think": False,  # Disable thinking mode — required for qwen3 models
            "options": {"temperature": 0.3, "num_predict": 100},
        }).encode()

        try:
            req = urllib.request.Request(
                f"{ollama_host}/api/chat",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = json.loads(resp.read().decode())
            raw = body.get("message", {}).get("content", "").strip()

            finding = fallback_finding
            fix = fallback_fix
            for line in raw.splitlines():
                stripped = line.strip()
                if stripped.upper().startswith("FINDING:"):
                    finding = stripped[len("FINDING:"):].strip()
                elif stripped.upper().startswith("FIX:"):
                    fix = stripped[len("FIX:"):].strip()

            result = {"finding": finding, "fix": fix}
        except Exception:
            # Ollama unavailable, slow, or error — fall back to static text gracefully
            result = {"finding": fallback_finding, "fix": fallback_fix}

        UserJourneyEngine._issue_ai_cache[cache_key] = result
        return result

    def _get_connection(self):
        return psycopg2.connect(**self.pg_kwargs)

    def get_available_dates(self):
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE '%events%'
        """)
        event_tables = [r[0] for r in cur.fetchall()]
        if not event_tables:
            cur.close()
            conn.close()
            return []

        queries = [f'SELECT DISTINCT to_char(event_time, \'YYYY-MM-DD\') AS day FROM "{t}" WHERE event_time IS NOT NULL' for t in event_tables]
        full_query = " UNION ".join(queries) + " ORDER BY 1 DESC"
        cur.execute(full_query)
        dates = [r[0] for r in cur.fetchall() if r[0]]
        cur.close()
        conn.close()
        return dates

    def fetch_all_events(self, date_filter=None, version_filter=None, profile_id=None):
        conn = self._get_connection()
        cur = conn.cursor()

        # Discover all event tables in the public schema
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name LIKE '%events%'
        """)
        tables = [r[0] for r in cur.fetchall()]

        all_events = []
        for table in tables:
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = %s
            """, (table,))
            cols = set(r[0] for r in cur.fetchall())

            # Mandatory columns
            if "profile_id" not in cols or "event_time" not in cols:
                continue

            query = f'''
                SELECT 
                    profile_id,
                    event_time,
                    event_timestamp,
                    event_type,
                    description,
                    screen_name,
                    '{table}' as log_source,
                    {'session_id' if 'session_id' in cols else "NULL"} as session_id,
                    {'autonym_script' if 'autonym_script' in cols else "NULL"} as autonym_script,
                    {'extra' if 'extra' in cols else "NULL"} as extra,
                    {'source_file' if 'source_file' in cols else "NULL"} as source_file,
                    {'device_model' if 'device_model' in cols else "NULL"} as device_model,
                    {'device_os' if 'device_os' in cols else "NULL"} as device_os,
                    {'app_version' if 'app_version' in cols else "NULL"} as app_version,
                    {'location_lat' if 'location_lat' in cols else "NULL"} as location_lat,
                    {'location_long' if 'location_long' in cols else "NULL"} as location_long,
                    {'category_title' if 'category_title' in cols else "NULL"} as category_title,
                    {'total_download_size_mb' if 'total_download_size_mb' in cols else "NULL"} as total_download_size_mb,
                    {'is_accepted' if 'is_accepted' in cols else "NULL"} as is_accepted,
                    {'question_id' if 'question_id' in cols else "NULL"} as question_id,
                    {'answer_id' if 'answer_id' in cols else "NULL"} as answer_id,
                    {'status' if 'status' in cols else "NULL"} as status
                FROM "{table}"
                WHERE event_time IS NOT NULL
            '''
            params = []
            if profile_id:
                query += " AND profile_id = %s"
                params.append(profile_id)

            cur.execute(query, tuple(params))
            col_names = [d[0] for d in cur.description]
            for row in cur.fetchall():
                row_dict = dict(zip(col_names, row))
                all_events.append(row_dict)

        # Check if sessions table exists
        sessions_table_data = []
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'sessions'
            )
        """)
        if cur.fetchone()[0]:
            cur.execute("SELECT * FROM sessions")
            scols = [d[0] for d in cur.description]
            sessions_table_data = [dict(zip(scols, r)) for r in cur.fetchall()]

        # Check if run_log table exists
        run_log_data = []
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = 'run_log'
            )
        """)
        if cur.fetchone()[0]:
            cur.execute("SELECT * FROM run_log ORDER BY 1 DESC LIMIT 20")
            rcols = [d[0] for d in cur.description]
            run_log_data = [dict(zip(rcols, r)) for r in cur.fetchall()]

        cur.close()
        conn.close()

        if not all_events:
            return pd.DataFrame(), sessions_table_data, run_log_data

        df = pd.DataFrame(all_events)
        df["event_time"] = pd.to_datetime(df["event_time"])
        # Standardize session_id from source_file if null
        def extract_session(row):
            if row["session_id"] and str(row["session_id"]).strip() not in ("", "None", "nan"):
                return str(row["session_id"])
            if row["source_file"] and "_" in str(row["source_file"]):
                parts = str(row["source_file"]).split("_")
                if len(parts) >= 2:
                    return parts[1]
            return "unknown"

        df["session_id"] = df.apply(extract_session, axis=1)

        # Apply date filter
        if date_filter and date_filter.lower() != "all":
            df["day"] = df["event_time"].dt.strftime("%Y-%m-%d")
            # Filter users whose first event or any event is on that day
            if date_filter.lower() == "yesterday":
                unique_days = sorted(df["day"].unique())
                target_day = unique_days[-2] if len(unique_days) >= 2 else unique_days[-1]
            else:
                target_day = date_filter

            # Find matching users active on target day
            matching_users = set(df[df["day"] == target_day]["profile_id"].unique())
            df = df[df["profile_id"].isin(matching_users)].copy()

        return df, sessions_table_data, run_log_data

    def generate_journey_report(self, date_filter="all", version_filter="4.0.0", profile_id=None):
        df_events, sessions_data, runs_log = self.fetch_all_events(date_filter=date_filter, profile_id=profile_id)
        if df_events.empty:
            return {"users": [], "stages": CANONICAL_STAGES, "screens": SCREEN_LABELS, "phases": PHASES, "issues": [], "files": [], "runs_log": []}

        # Group by user
        users_grouped = df_events.groupby("profile_id")
        users_data = []

        all_days = sorted(df_events["event_time"].dt.strftime("%Y-%m-%d").unique())
        yesterday_str = all_days[-2] if len(all_days) >= 2 else (all_days[-1] if all_days else "")

        user_tab_idx = 1
        for pid, uevents in users_grouped:
            uevents = uevents.sort_values("event_time").reset_index(drop=True)
            
            # Metadata resolution
            device = "Unknown"
            os_name = "Android"
            app_version = "Unknown"
            app_iid = ""
            loc = None

            device_series = uevents["device_model"].dropna()
            if not device_series.empty:
                device = str(device_series.iloc[0])

            os_series = uevents["device_os"].dropna()
            if not os_series.empty:
                os_name = str(os_series.iloc[0])

            ver_series = uevents["app_version"].dropna()
            if not ver_series.empty:
                app_version = str(ver_series.iloc[0])

            is_emu = bool(re.search(r"sdk_|emulator|gphone|goog3", device.lower()))

            lat_series = uevents["location_lat"].dropna()
            long_series = uevents["location_long"].dropna()
            if not lat_series.empty and not long_series.empty:
                lat = float(lat_series.iloc[0])
                lon = float(long_series.iloc[0])
                area = "Kumasi area, Ghana" if (abs(lat - 6.8) < 1 and abs(lon - (-1.5)) < 1) else ("Accra area, Ghana" if (abs(lat - 5.5) < 1 and abs(lon - (-0.2)) < 1) else "Location registered")
                loc = {"lat": lat, "lon": lon, "area": area}

            # If version filter is active, filter out non-matching users
            if version_filter and version_filter.lower() != "all" and app_version != "Unknown":
                if app_version != version_filter:
                    continue

            user_first_dt = uevents["event_time"].iloc[0]
            user_last_dt = uevents["event_time"].iloc[-1]
            cohort_day = user_first_dt.strftime("%Y-%m-%d")
            active_days = sorted(uevents["event_time"].dt.strftime("%Y-%m-%d").unique().tolist())
            user_span_sec = (user_last_dt - user_first_dt).total_seconds()
            user_span_str = format_human_duration(user_span_sec)

            # Build stage timestamps
            stages_ts = [None] * len(CANONICAL_STAGES)

            # Stage 1: App launched
            launch_rows = uevents[(uevents["event_type"].isin(["started", "installed"])) | (uevents["screen_name"] == "splash_screen")]
            if not launch_rows.empty:
                stages_ts[0] = launch_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            else:
                stages_ts[0] = user_first_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 2: Language screen
            lang_scr_rows = uevents[uevents["screen_name"] == "language_list_screen"]
            if not lang_scr_rows.empty:
                stages_ts[1] = lang_scr_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 3: Language download chosen
            lang_dl_rows = uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"].isin(["downloaded", "selected"])) & (uevents["status"].isin(["completed", "selected"]))]
            if not lang_dl_rows.empty:
                stages_ts[2] = lang_dl_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 4: Category screen
            cat_scr_rows = uevents[uevents["screen_name"] == "category_list_screen"]
            if not cat_scr_rows.empty:
                stages_ts[3] = cat_scr_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 5: Category download pressed
            cat_dl_rows = uevents[(uevents["log_source"] == "download_category_events") & (uevents["event_type"] == "downloaded")]
            if not cat_dl_rows.empty:
                stages_ts[4] = cat_dl_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 6: Onboarding carousel
            onb_scr_rows = uevents[uevents["screen_name"] == "onboarding_screen"]
            if not onb_scr_rows.empty:
                stages_ts[5] = onb_scr_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 7: T&C accepted
            tc_rows = uevents[(uevents["event_type"] == "accepted") | (uevents["is_accepted"] == "1")]
            if not tc_rows.empty:
                stages_ts[6] = tc_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 8: Survey started
            survey_start = uevents[(uevents["screen_name"] == "onboarding_survey_screen")]
            if not survey_start.empty:
                stages_ts[7] = survey_start.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 9: Survey completed
            survey_end = uevents[(uevents["screen_name"] == "onboarding_survey_screen") & (uevents["event_type"] == "completed")]
            survey_answers = uevents[(uevents["screen_name"] == "onboarding_survey_screen") & (uevents["event_type"] == "answered")]
            if not survey_end.empty:
                stages_ts[8] = survey_end.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            elif len(survey_answers) >= 6:
                stages_ts[8] = survey_answers.iloc[-1]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 10: Onboarding completed
            onb_comp = uevents[(uevents["screen_name"] == "onboarding_complete_screen") & (uevents["event_type"] == "completed")]
            if not onb_comp.empty:
                stages_ts[9] = onb_comp.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Stage 11: Home screen reached
            home_rows = uevents[uevents["screen_name"] == "home_screen"]
            if not home_rows.empty:
                stages_ts[10] = home_rows.iloc[0]["event_time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            # Furthest stage calculation
            furthest = 1
            for i, ts in enumerate(stages_ts):
                if ts is not None:
                    furthest = i + 1

            outcome = "Completed" if furthest == 11 else "Dropped"

            # Breakpoint diagnosis
            if outcome == "Completed":
                bp = "Completed"
                first_t = user_first_dt
                home_t = pd.to_datetime(stages_ts[10])
                time_to_home = format_human_duration((home_t - first_t).total_seconds())
                broke = f"Completed onboarding in {time_to_home} and reached Home"
            elif furthest in (1, 2):
                bp = "Language screen"
                lsel = len(uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"] == "selected")])
                lcan = len(uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"] == "cancelled")])
                if lsel == 0 and lcan == 0:
                    broke = "Language screen shown, then no further action"
                else:
                    broke = f"Never left Language screen ({lsel} picks, {lcan} cancels)"
            elif furthest in (3, 4):
                bp = "Category screen"
                # Check if went back to language
                after_cat = uevents[uevents.index > cat_scr_rows.index[0]] if not cat_scr_rows.empty else pd.DataFrame()
                ctog = len(uevents[(uevents["log_source"] == "download_category_events") & (uevents["event_type"] == "selected")])
                if not after_cat.empty and (after_cat["screen_name"] == "language_list_screen").any():
                    broke = f"Went back from Categories to Language and left"
                else:
                    broke = f"Left Category screen without pressing Download"
            elif furthest in (5, 6):
                bp = "Onboarding assets"
                size = cat_dl_rows.iloc[0]["total_download_size_mb"] if not cat_dl_rows.empty and cat_dl_rows.iloc[0]["total_download_size_mb"] else "200"
                try:
                    size_mb = int(float(size))
                except Exception:
                    size_mb = 200
                broke = f"Stuck on 'Preparing language assets' ({size_mb} MB); never reached T&C"
            else:
                bp = "Survey"
                broke = "Dropped during survey / onboarding"

            # Diagnostic counts & event flags
            events_list = []
            prev_time = None
            launches = 0
            bgs = 0
            prev_screen = None
            tc_accept_count = 0
            lang_cancels_timing = []
            seen_screens = set()

            for idx, erow in uevents.iterrows():
                cur_time = erow["event_time"]
                gap = (cur_time - prev_time).total_seconds() if prev_time else None
                elapsed = (cur_time - user_first_dt).total_seconds()
                prev_time = cur_time

                scr = str(erow["screen_name"]) if erow["screen_name"] else "unknown"
                etype = str(erow["event_type"]) if erow["event_type"] else "interaction"
                desc = str(erow["description"]) if erow["description"] else f"{etype} on {scr}"
                log_src = str(erow["log_source"]).replace("_events", "")
                detail = ""

                flags = []

                if etype == "installed":
                    flags.append("Installed")
                if etype == "started":
                    launches += 1
                    flags.append("App launched")
                if etype in ("suspended", "app_background"):
                    bgs += 1
                    flags.append(f"App backgrounded on {SCREEN_LABELS.get(scr, scr)}")

                if scr not in seen_screens:
                    seen_screens.add(scr)
                    flags.append(f"→ {SCREEN_LABELS.get(scr, scr)}")
                elif prev_screen and scr != prev_screen:
                    # Checked if it's a BACK navigation
                    if scr in ("language_list_screen", "category_list_screen") and prev_screen in ("category_list_screen", "onboarding_screen"):
                        flags.append(f"BACK to {SCREEN_LABELS.get(scr, scr)}")

                if etype == "accepted":
                    tc_accept_count += 1
                    if tc_accept_count > 1:
                        flags.append(f"Duplicate T&C accept #{tc_accept_count}")

                if gap is not None:
                    if gap >= 60:
                        m_gap = int(gap // 60)
                        flags.append(f"Came back after {m_gap}m {int(gap % 60):02d}s")
                    elif gap >= 20:
                        flags.append(f"Idle {gap:.1f}s")

                # Cancellation timing check
                if log_src == "download_language" and etype == "cancelled":
                    # Check gap to preceding downloaded/selected event
                    if gap is not None and gap < 5.0:
                        flags.append(f"Download cancelled {gap:.1f}s after start")
                        lang_cancels_timing.append(gap)

                if idx == len(uevents) - 1:
                    flags.append("LAST EVENT")

                # Build detail description
                if pd.notna(erow["category_title"]):
                    detail = str(erow["category_title"])
                elif pd.notna(erow["question_id"]) and pd.notna(erow["answer_id"]):
                    detail = f"{erow['question_id']} → {erow['answer_id']}"
                elif pd.notna(erow["total_download_size_mb"]):
                    detail = f"Download size: {erow['total_download_size_mb']} MB"

                events_list.append({
                    "n": idx + 1,
                    "ist": cur_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "utc": (cur_time - datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "ts": int(cur_time.timestamp() * 1000),
                    "gap": round(gap, 3) if gap is not None else None,
                    "el": round(elapsed, 3),
                    "scr": scr,
                    "ph": get_screen_phase(scr),
                    "log": log_src,
                    "type": etype,
                    "desc": desc,
                    "lang": str(erow["autonym_script"]) if pd.notna(erow["autonym_script"]) else "",
                    "det": detail,
                    "ses": str(erow["session_id"])[:8],
                    "flags": flags,
                })
                prev_screen = scr

            # Screen runs grouping
            runs = []
            current_run = None
            for ev in events_list:
                scr = ev["scr"]
                if current_run is None or current_run["scr"] != scr:
                    if current_run is not None:
                        runs.append(current_run)
                    current_run = {
                        "scr": scr,
                        "ph": ev["ph"],
                        "start": ev["ts"],
                        "end": ev["ts"],
                        "n": 1,
                        "counts": {ev["type"]: 1},
                        "keys": [f"{ev['ist'][11:19]} {ev['desc']}"],
                    }
                else:
                    current_run["end"] = ev["ts"]
                    current_run["n"] += 1
                    current_run["counts"][ev["type"]] = current_run["counts"].get(ev["type"], 0) + 1
                    if len(current_run["keys"]) < 4:
                        current_run["keys"].append(f"{ev['ist'][11:19]} {ev['desc']}")
            if current_run:
                runs.append(current_run)

            # Summaries and Key finding
            lsel_cnt = len(uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"] == "selected")])
            lcan_cnt = len(uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"] == "cancelled")])
            ctog_cnt = len(uevents[(uevents["log_source"] == "download_category_events") & (uevents["event_type"] == "selected")])
            cdl_cnt = len(uevents[(uevents["log_source"] == "download_category_events") & (uevents["event_type"] == "downloaded")])

            key_issue = ""
            if outcome == "Completed":
                if tc_accept_count > 1:
                    key_issue = f"T&C 'accepted' fired {tc_accept_count}× on slide 2"
                else:
                    key_issue = "Completed onboarding smoothly"
            elif bp == "Category screen":
                key_issue = f"{ctog_cnt} category changes, no Download"
            elif bp == "Language screen":
                if lang_cancels_timing:
                    avg_c = sum(lang_cancels_timing) / len(lang_cancels_timing)
                    key_issue = f"Every language download cancelled within {avg_c:.1f}s"
                else:
                    key_issue = "No further events after the Language screen"
            elif bp == "Onboarding assets":
                key_issue = "Asset download on onboarding never finished"

            # Facts bullet generation
            facts = [
                f"First seen {user_first_dt.strftime('%d %b %H:%M:%S')} IST on v{app_version} ({device}{', emulator' if is_emu else ''}).",
            ]
            if lsel_cnt > 0 or lcan_cnt > 0:
                facts.append(f"Language screen: {lsel_cnt} picks, {lcan_cnt} cancels.")
            if ctog_cnt > 0 or cdl_cnt > 0:
                facts.append(f"Categories: {ctog_cnt} selection changes, Download pressed {cdl_cnt}×.")
            if outcome == "Completed":
                facts.append(f"Completed onboarding and reached Home at +{format_human_duration((pd.to_datetime(stages_ts[10]) - user_first_dt).total_seconds())}.")
            last_ev = events_list[-1] if events_list else {}
            facts.append(f"Exit: {last_ev.get('desc', 'Left app')} at {user_last_dt.strftime('%d %b %H:%M:%S')} IST.")

            # Selected language
            content_lang = ""
            lang_candidates = uevents["autonym_script"].dropna()
            if not lang_candidates.empty:
                content_lang = str(lang_candidates.iloc[-1])

            # Session objects
            user_sessions = []
            for s_id, s_events in uevents.groupby("session_id"):
                s_events = s_events.sort_values("event_time")
                s_start = s_events["event_time"].iloc[0].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                s_end = s_events["event_time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                span_s = (s_events["event_time"].iloc[-1] - s_events["event_time"].iloc[0]).total_seconds()
                user_sessions.append({
                    "id": str(s_id)[:8],
                    "start": s_start,
                    "end": s_end,
                    "dur": round(span_s, 1),
                    "span": round(span_s, 1),
                    "count": len(s_events),
                    "actual": len(s_events),
                    "ver": app_version,
                    "bad": False,
                })

            users_data.append({
                "tab": f"U{user_tab_idx:02d}",
                "pid": pid,
                "cohort": cohort_day,
                "days": active_days,
                "versions": [app_version] if app_version != "Unknown" else ["4.0.0"],
                "ver": app_version if app_version != "Unknown" else "4.0.0",
                "device": device,
                "os": os_name,
                "emu": is_emu,
                "iid": app_iid or pid,
                "location": loc,
                "first": user_first_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "last": user_last_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "span": user_span_str,
                "n": len(uevents),
                "launches": max(1, launches),
                "bgs": bgs,
                "stages": stages_ts,
                "furthest": furthest,
                "outcome": outcome,
                "bp": bp,
                "broke": broke,
                "key": key_issue,
                "facts": facts,
                "exit_type": "backgrounded" if bgs > 0 else "silent",
                "exit_txt": f"{last_ev.get('desc', 'App closed')} on {SCREEN_LABELS.get(last_ev.get('scr'), 'screen')}",
                "last_screen": SCREEN_LABELS.get(last_ev.get("scr"), "screen"),
                "last_ph": last_ev.get("ph", "app"),
                "last_event": f"{last_ev.get('type')}: {last_ev.get('desc')}",
                "lang": content_lang,
                "post_home": [SCREEN_LABELS.get(s, s) for s in uevents[uevents["event_time"] > pd.to_datetime(stages_ts[10])]["screen_name"].dropna().unique()] if stages_ts[10] else [],
                "counts": {
                    "lsel": lsel_cnt,
                    "lcan": lcan_cnt,
                    "wv": len(uevents[(uevents["log_source"] == "download_language_events") & (uevents["event_type"] == "downloaded")]),
                    "nv": 0,
                    "cancel_after": [round(c, 1) for c in lang_cancels_timing[:3]],
                    "ctog": ctog_cnt,
                    "cdl": cdl_cnt,
                    "sizes": [float(r["total_download_size_mb"]) for _, r in cat_dl_rows.iterrows() if pd.notna(r["total_download_size_mb"]) and r["total_download_size_mb"]],
                    "prep": len(uevents[uevents["description"].str.contains("Preparing language assets", na=False)]),
                    "bounces": 0,
                    "disc": len(uevents[uevents["screen_name"] == "settings_disclaimer_screen"]),
                    "tc": tc_accept_count,
                    "ans": len(survey_answers),
                    "login_ok": 1 if outcome == "Completed" else 0,
                    "login_fail": 0,
                    "login_try": 0,
                    "modules": [],
                    "videos": 0,
                    "vid_fail": 0,
                    "vid_start": 0,
                    "quiz": 0,
                    "quiz_fail": 0,
                    "quiz_null": 0,
                    "quiz_scores": [],
                    "clin": 0,
                    "oc_bg": 0,
                    "replay": 0,
                    "lang_repeat": 0,
                },
                "sessions": user_sessions,
                "runs": runs,
                "events": events_list,
            })
            user_tab_idx += 1

        # Generate automated issues
        issues = self._detect_issues(users_data)

        # Assemble full dataset D
        min_win = df_events["event_time"].min().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        max_win = df_events["event_time"].max().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        report_dict = {
            "users": users_data,
            "stages": CANONICAL_STAGES,
            "screens": SCREEN_LABELS,
            "phases": PHASES,
            "issues": issues,
            "files": [],
            "runs_log": runs_log,
            "days": all_days,
            "yesterday": yesterday_str,
            "report_day": datetime.datetime.now().strftime("%Y-%m-%d"),
            "window": [min_win, max_win],
            "filter": {
                "version": version_filter or "all",
                "since": all_days[0] if all_days else "",
                "excluded": [],
            },
            "total": len(df_events),
            "sessions_total": sum(len(u["sessions"]) for u in users_data),
        }
        return report_dict

    def _detect_issues(self, users):
        issues = []
        total_users = max(len(users), 1)  # avoid division by zero

        # P0: Onboarding asset download stall
        stuck_asset_users = [u["pid"] for u in users if u["bp"] == "Onboarding assets"]
        if stuck_asset_users:
            pct = round(len(stuck_asset_users) / total_users * 100, 1)
            ai = self._ai_enhance_issue(
                area="Onboarding asset download stall",
                facts={
                    "users_stalled": len(stuck_asset_users),
                    "total_users_in_cohort": total_users,
                    "percent_affected": f"{pct}%",
                    "stall_point": "Preparing language assets screen",
                    "outcome": "never reached Terms & Conditions",
                },
                fallback_finding="The category asset download blocks onboarding until it finishes. Several users never got past it.",
                fallback_fix="Let users reach Home while the download continues in the background, and show a clear progress bar.",
            )
            issues.append({
                "sev": "P0",
                "area": "Onboarding asset download",
                "finding": ai["finding"],
                "evidence": f"{len(stuck_asset_users)} user(s) ({pct}%) stalled on 'Preparing language assets' and never reached T&C.",
                "users": stuck_asset_users,
                "fix": ai["fix"],
            })

        # P0: Language download cancellation loops
        lang_cancel_users = [u["pid"] for u in users if u["bp"] == "Language screen" and u["counts"]["lcan"] >= 10]
        if lang_cancel_users:
            pct = round(len(lang_cancel_users) / total_users * 100, 1)
            avg_cancels = round(
                sum(u["counts"]["lcan"] for u in users if u["pid"] in lang_cancel_users)
                / max(len(lang_cancel_users), 1), 1
            )
            ai = self._ai_enhance_issue(
                area="Language download cancellation loop",
                facts={
                    "users_affected": len(lang_cancel_users),
                    "total_users_in_cohort": total_users,
                    "percent_affected": f"{pct}%",
                    "avg_cancellations_per_user": avg_cancels,
                    "threshold_used": "10+ rapid picks & cancels",
                    "outcome": "user never left the language screen",
                },
                fallback_finding="Language selection has high cancellation bursts within seconds of tapping.",
                fallback_fix="Check download prompt button behavior and network timeout thresholds.",
            )
            issues.append({
                "sev": "P0",
                "area": "Language download loop",
                "finding": ai["finding"],
                "evidence": f"{len(lang_cancel_users)} user(s) ({pct}%) had {avg_cancels}+ avg language picks & cancels and never left the screen.",
                "users": lang_cancel_users,
                "fix": ai["fix"],
            })

        # P1: Category screen drop-off
        cat_drop_users = [u["pid"] for u in users if u["bp"] == "Category screen"]
        if cat_drop_users:
            pct = round(len(cat_drop_users) / total_users * 100, 1)
            avg_toggles = round(
                sum(u["counts"].get("ccat", 0) for u in users if u["pid"] in cat_drop_users)
                / max(len(cat_drop_users), 1), 1
            )
            ai = self._ai_enhance_issue(
                area="Category screen drop-off without downloading",
                facts={
                    "users_dropped": len(cat_drop_users),
                    "total_users_in_cohort": total_users,
                    "percent_dropped": f"{pct}%",
                    "avg_category_toggles_per_user": avg_toggles,
                    "outcome": "left without pressing the Download button",
                },
                fallback_finding="Users toggle categories multiple times but leave without starting the download.",
                fallback_fix="Make the primary 'Download' CTA visually persistent and show total download size dynamically.",
            )
            issues.append({
                "sev": "P1",
                "area": "Category screen",
                "finding": ai["finding"],
                "evidence": f"{len(cat_drop_users)} user(s) ({pct}%) made avg {avg_toggles} category toggles but did not press Download.",
                "users": cat_drop_users,
                "fix": ai["fix"],
            })

        # P1: Duplicate T&C accepts
        dup_tc_users = [u["pid"] for u in users if u["counts"]["tc"] > 1]
        if dup_tc_users:
            pct = round(len(dup_tc_users) / total_users * 100, 1)
            max_dups = max((u["counts"]["tc"] for u in users if u["pid"] in dup_tc_users), default=2)
            ai = self._ai_enhance_issue(
                area="Duplicate Terms & Conditions accepted events",
                facts={
                    "users_with_duplicates": len(dup_tc_users),
                    "total_users_in_cohort": total_users,
                    "percent_affected": f"{pct}%",
                    "max_duplicate_count_observed": max_dups,
                    "issue": "T&C accepted event fires more than once per user on the same onboarding slide",
                },
                fallback_finding="The onboarding T&C 'accepted' event fires repeatedly on the same slide.",
                fallback_fix="Disable or debounce the accept button once tapped and log T&C accept once per user.",
            )
            issues.append({
                "sev": "P1",
                "area": "T&C acceptance",
                "finding": ai["finding"],
                "evidence": f"{len(dup_tc_users)} user(s) ({pct}%) fired duplicate T&C accepted events (max {max_dups}×).",
                "users": dup_tc_users,
                "fix": ai["fix"],
            })

        # Info: Emulator / Test traffic
        emu_users = [u["pid"] for u in users if u["emu"]]
        if emu_users:
            pct = round(len(emu_users) / total_users * 100, 1)
            ai = self._ai_enhance_issue(
                area="Emulator and test device traffic in production cohort",
                facts={
                    "emulator_profiles": len(emu_users),
                    "total_users_in_cohort": total_users,
                    "percent_of_cohort": f"{pct}%",
                    "device_models": "sdk_goog3 or sdk_gphone (Android SDK emulators)",
                    "risk": "skews conversion metrics and funnel drop-off rates",
                },
                fallback_finding="Several profiles are on Android SDK emulators representing internal QA runs.",
                fallback_fix="Tag and filter emulator sessions so production cohort conversion is not skewed.",
            )
            issues.append({
                "sev": "Info",
                "area": "Test devices",
                "finding": ai["finding"],
                "evidence": f"{len(emu_users)} profile(s) ({pct}%) running on sdk_goog3 or sdk_gphone.",
                "users": emu_users,
                "fix": ai["fix"],
            })

        return issues

    def generate_markdown_summary(self, rep: dict) -> str:
        users = rep.get("users", [])
        if not users:
            return "No users found for the specified criteria."

        total_users = len(users)
        completed = [u for u in users if u["outcome"] == "Completed"]
        dropped = [u for u in users if u["outcome"] != "Completed"]
        comp_rate = (len(completed) / total_users * 100) if total_users else 0

        # Breakpoints breakdown
        bp_counts = defaultdict(int)
        for u in users:
            bp_counts[u["bp"]] += 1

        top_issues = rep.get("issues", [])
        p0_issues = [i for i in top_issues if i["sev"] == "P0"]

        md = []
        md.append(f"### 📊 Safe Delivery Onboarding & User Journey Report")
        md.append(f"**Cohort Period**: {rep.get('window', ['N/A', 'N/A'])[0][:10]} to {rep.get('window', ['N/A', 'N/A'])[1][:10]} | **Total Users**: {total_users}")
        md.append(f"- **Reached Home (Completed)**: {len(completed)} ({comp_rate:.1f}%)")
        md.append(f"- **Dropped Off**: {len(dropped)} ({100 - comp_rate:.1f}%)")
        md.append(f"- **App Versions**: {', '.join(sorted(set(u['ver'] for u in users)))}")
        md.append("")
        md.append("#### 📍 Where Users Dropped Off (Furthest Stage Reached)")
        for bp, count in sorted(bp_counts.items(), key=lambda x: -x[1]):
            pct = (count / total_users) * 100
            status_icon = "✅" if bp == "Completed" else "⚠️"
            md.append(f"- {status_icon} **{bp}**: {count} user(s) ({pct:.1f}%)")

        if p0_issues:
            md.append("")
            md.append("#### 🚨 Critical Issues & Drop-off Blockers (P0)")
            for iss in p0_issues:
                md.append(f"- **{iss['area']}**: {iss['finding']}")
                md.append(f"  *Evidence*: {iss['evidence']}")
                md.append(f"  *Impacted Users*: {', '.join([u[:15] for u in iss['users'][:4]])}")
                md.append(f"  *Fix*: {iss['fix']}")

        md.append("")
        md.append("#### 👥 User Breakdown Snapshot")
        for u in users[:8]:
            status_badge = "🟢 Reached Home" if u["outcome"] == "Completed" else f"🔴 Dropped at {u['bp']}"
            md.append(f"- `{u['pid'][:18]}` ({u['device']}, v{u['ver']}): {status_badge} — _{u['broke']}_")
        if len(users) > 8:
            md.append(f"_...and {len(users) - 8} more users._")

        return "\n".join(md)

    def render_html_report(self, rep: dict) -> str:
        data_json = json.dumps(rep, default=str)
        html = f"""<!doctype html><html><head><meta charset=utf8><meta name=viewport content="width=device-width,initial-scale=1,viewport-fit=cover"><style>:root{{color-scheme:light;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}}html{{scroll-padding-top:env(safe-area-inset-top,0px)}}body{{margin:0;padding:0;font:14px -apple-system,BlinkMacSystemFont,sans-serif;background:#FAFAFA;color:#1D1D1B}}img{{max-width:100%}}[hidden]:not([hidden=until-found i]){{display:none!important}}</style></head><body>
<title>Maternity Foundation · Safe Delivery Onboarding Flows</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap">
<style>
:root{{
  --ground:#FAFAFA; --surface:#FFFFFF; --sunk:#F2F2F2; --ink:#1D1D1B; --ink2:#444444; --muted:#777777; --line:#E5E5E5; --line2:#D0D0D0;
  --accent:#A80041; --accent-soft:#F9E6EE;
  --good:#1B7D50; --good-bg:#EBF7F0; --bad:#A80041; --bad-bg:#FDF0F4; --warn:#C05621; --warn-bg:#FEF5ED; --p2:#7D0030; --p2-bg:#FCE7F3;
  --s-splash:#777777; --s-language:#C05621; --s-category:#1B7D50; --s-onboarding:#7D0030; --s-survey:#A80041;
  --s-app:#006699; --s-app-bg:#E6F4FA; --s-splash-bg:#F2F2F2; --s-language-bg:#FEF5ED; --s-category-bg:#EBF7F0; --s-onboarding-bg:#FCE7F3; --s-survey-bg:#F9E6EE;
  --shadow:0 1px 3px rgba(0,0,0,.07),0 4px 14px rgba(0,0,0,.05);
  --sans:"Montserrat",system-ui,-apple-system,"Segoe UI",sans-serif;
  --cond:"Montserrat",system-ui,-apple-system,sans-serif;
  --mono:"IBM Plex Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{
  color-scheme:dark;
  --ground:#141414; --surface:#1E1E1E; --sunk:#282828; --ink:#F0F0F0; --ink2:#CCCCCC; --muted:#888888; --line:#333333; --line2:#444444;
  --accent:#D9004B; --accent-soft:#3D0017;
  --good:#34D399; --good-bg:#15301F; --bad:#F87171; --bad-bg:#3A1A17; --warn:#FBBF24; --warn-bg:#35270F; --p2:#F472B6; --p2-bg:#2A1A3E;
  --s-splash:#888888; --s-language:#FBBF24; --s-category:#34D399; --s-onboarding:#F472B6; --s-survey:#D9004B;
  --s-app:#38BDF8; --s-app-bg:#152A33; --s-splash-bg:#282828; --s-language-bg:#35270F; --s-category-bg:#15301F; --s-onboarding-bg:#2A1A3E; --s-survey-bg:#3D0017;
  --shadow:0 1px 3px rgba(0,0,0,.4),0 4px 14px rgba(0,0,0,.3);
}}}}
:root[data-theme="dark"]{{
  color-scheme:dark;
  --ground:#141414; --surface:#1E1E1E; --sunk:#282828; --ink:#F0F0F0; --ink2:#CCCCCC; --muted:#888888; --line:#333333; --line2:#444444;
  --accent:#D9004B; --accent-soft:#3D0017;
  --good:#34D399; --good-bg:#15301F; --bad:#F87171; --bad-bg:#3A1A17; --warn:#FBBF24; --warn-bg:#35270F; --p2:#F472B6; --p2-bg:#2A1A3E;
  --s-splash:#888888; --s-language:#FBBF24; --s-category:#34D399; --s-onboarding:#F472B6; --s-survey:#D9004B;
  --s-app:#38BDF8; --s-app-bg:#152A33; --s-splash-bg:#282828; --s-language-bg:#35270F; --s-category-bg:#15301F; --s-onboarding-bg:#2A1A3E; --s-survey-bg:#3D0017;
  --shadow:0 1px 3px rgba(0,0,0,.4),0 4px 14px rgba(0,0,0,.3);
}}
*{{box-sizing:border-box}}
body{{background:var(--ground);color:var(--ink);font-family:var(--sans);font-size:14px;line-height:1.5;margin:0;-webkit-font-smoothing:antialiased}}
.wrap{{max-width:1320px;margin:0 auto;padding-inline:20px;padding-block:0 48px}}
button,input,select{{font:inherit;color:inherit}}
a{{color:var(--accent)}}
:focus-visible{{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}}
.mono{{font-family:var(--mono);font-size:12.5px}}
.tnum{{font-variant-numeric:tabular-nums}}

/* header */
.top{{position:sticky;top:env(safe-area-inset-top,0px);z-index:20;background:#A80041;border-bottom:3px solid #7D0030;box-shadow:0 2px 10px rgba(0,0,0,.15)}}
.top .wrap{{display:flex;flex-wrap:wrap;align-items:center;gap:8px 24px;padding-block:12px}}
.brand{{display:flex;flex-direction:column;gap:1px;margin-right:auto}}
.brand b{{font-family:var(--cond);font-size:18px;font-weight:700;letter-spacing:-.01em;line-height:1.2;color:#ffffff}}
.brand span{{color:rgba(255,255,255,.8);font-size:12px}}
.tabs{{display:flex;gap:6px;flex-wrap:wrap}}
.tabs button{{border:1px solid rgba(255,255,255,.3);background:rgba(255,255,255,.12);padding:7px 16px;border-radius:4px;cursor:pointer;color:#ffffff;font-weight:600;font-size:12.5px;text-transform:uppercase;letter-spacing:.04em;backdrop-filter:blur(4px);transition:all .15s ease}}
.tabs button:hover{{background:rgba(255,255,255,.25);border-color:rgba(255,255,255,.6)}}
.tabs button[aria-selected="true"]{{background:#ffffff;color:#A80041;border-color:#ffffff;font-weight:700}}

h2{{font-family:var(--cond);font-size:17px;margin:0;letter-spacing:.005em;text-wrap:balance}}
h3{{font-size:13px;margin:0;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}}
.sec{{margin-top:28px}}
.sec-h{{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:12px}}
.sec-h p{{margin:0;color:var(--muted);font-size:12.5px}}
.panel{{background:var(--surface);border:1px solid var(--line);border-radius:10px;box-shadow:var(--shadow)}}
.pad{{padding:18px}}
.note{{color:var(--muted);font-size:12px;margin:10px 0 0}}

/* pills */
.pill{{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:600;padding:2px 9px;border-radius:999px;white-space:nowrap}}
.pill.good{{background:var(--good-bg);color:var(--good)}}
.pill.bad{{background:var(--bad-bg);color:var(--bad)}}
.pill.warn{{background:var(--warn-bg);color:var(--warn)}}
.pill.neutral{{background:var(--sunk);color:var(--ink2)}}
.pill.ver{{background:var(--accent-soft);color:var(--accent);font-family:var(--mono);font-weight:500}}
.pill.ver.unknown{{background:var(--warn-bg);color:var(--warn)}}
.dot{{width:8px;height:8px;border-radius:50%;display:inline-block;flex:none}}
.sev{{font-family:var(--mono);font-size:11px;font-weight:500;padding:2px 7px;border-radius:4px}}
.sev.P0{{background:var(--bad);color:var(--surface)}}
.sev.P1{{background:var(--warn);color:var(--surface)}}
.sev.P2{{background:var(--p2-bg);color:var(--p2)}}
.sev.Info{{background:var(--sunk);color:var(--muted)}}
.scr{{display:inline-flex;align-items:center;gap:6px;font-size:12px;padding:1px 8px;border-radius:4px;white-space:nowrap;font-weight:500}}

/* KPIs */
.kpis{{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin-top:20px}}
.kpi{{padding:14px 16px}}
.kpi small{{display:block;color:var(--muted);font-size:11.5px;text-transform:uppercase;letter-spacing:.06em;font-weight:600}}
.kpi b{{display:block;font-family:var(--cond);font-size:34px;line-height:1.1;margin-top:4px;font-variant-numeric:tabular-nums}}
.kpi em{{font-style:normal;color:var(--muted);font-size:12px}}
.kpi.good b{{color:var(--good)}} .kpi.bad b{{color:var(--bad)}}

.grid2{{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,1fr);gap:16px}}

/* bars */
.bars{{display:grid;gap:7px}}
.bar{{display:grid;grid-template-columns:minmax(150px,210px) 1fr minmax(70px,auto);align-items:center;gap:12px;font-size:13px}}
.bar .lbl{{color:var(--ink2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.bar .lbl i{{font-style:normal;color:var(--muted);font-family:var(--mono);font-size:11.5px;margin-right:6px}}
.track{{height:20px;background:var(--sunk);border-radius:4px;position:relative;overflow:hidden}}
.fill{{height:100%;border-radius:4px;background:var(--accent);display:flex;align-items:center;justify-content:flex-end;padding-right:6px;color:var(--surface);font-size:11.5px;font-weight:600;font-family:var(--mono);min-width:22px}}
.bar .lost{{font-size:12px;text-align:right;font-family:var(--mono);color:var(--muted)}}
.bar .lost.hot{{color:var(--bad);font-weight:600}}

/* matrix */
.tblwrap{{overflow-x:auto}}
table{{border-collapse:collapse;width:100%}}
th{{font-size:11.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);font-weight:600;text-align:left;padding:9px 10px;border-bottom:1px solid var(--line2);white-space:nowrap;background:var(--surface);position:sticky;top:0;z-index:1}}
td{{padding:8px 10px;border-bottom:1px solid var(--line);vertical-align:top}}
tr.click{{cursor:pointer}}
tr.click:hover td{{background:var(--sunk)}}
.matrix td.st{{text-align:center;padding:8px 4px}}
.matrix th.st{{text-align:center;font-size:10px;line-height:1.25;white-space:normal;min-width:58px;max-width:72px;vertical-align:bottom;letter-spacing:.02em}}
.matrix td{{vertical-align:middle}}
.cell{{display:inline-block;width:18px;height:18px;border-radius:4px}}
.cell.y{{background:var(--good)}} .cell.n{{background:var(--sunk);border:1px dashed var(--line2)}}
.cell.stop{{background:var(--bad)}}

/* issues */
.issues{{display:grid;gap:12px}}
.issue{{display:grid;grid-template-columns:4px 1fr;overflow:hidden}}
.issue .stripe{{background:var(--line2)}}
.issue.P0 .stripe{{background:var(--bad)}} .issue.P1 .stripe{{background:var(--warn)}} .issue.P2 .stripe{{background:var(--p2)}}
.issue .body{{padding:14px 18px;display:grid;gap:8px}}
.issue .head{{display:flex;gap:10px;align-items:center;flex-wrap:wrap}}
.issue .area{{font-size:12px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.05em}}
.issue .finding{{font-weight:600;font-size:14.5px;max-width:95ch}}
.issue dl{{display:grid;grid-template-columns:110px 1fr;gap:4px 14px;margin:0;font-size:13px}}
.issue dt{{color:var(--muted)}} .issue dd{{margin:0;color:var(--ink2);max-width:110ch}}
.ulinks{{display:flex;flex-wrap:wrap;gap:6px}}
.ulink{{font-family:var(--mono);font-size:12px;background:var(--sunk);border:1px solid var(--line);border-radius:4px;padding:0 6px;cursor:pointer;color:var(--accent)}}
.ulink:hover{{border-color:var(--accent)}}
.filters{{display:flex;gap:6px;flex-wrap:wrap}}
.chipbtn{{border:1px solid var(--line2);background:var(--surface);padding:4px 11px;border-radius:999px;cursor:pointer;font-size:12.5px;color:var(--ink2)}}
.chipbtn[aria-pressed="true"]{{background:var(--ink);color:var(--ground);border-color:var(--ink)}}

/* users view */
.uview{{display:grid;grid-template-columns:300px minmax(0,1fr);gap:16px;margin-top:20px;align-items:start}}
.ulist{{position:sticky;top:calc(env(safe-area-inset-top,0px) + 76px);max-height:calc(100vh - 100px);display:flex;flex-direction:column}}
.ulist .ctrl{{padding:12px;border-bottom:1px solid var(--line);display:grid;gap:8px}}
.ulist input{{width:100%;padding:7px 10px;border:1px solid var(--line2);border-radius:6px;background:var(--surface)}}
.ulist ul{{list-style:none;margin:0;padding:6px;overflow:auto}}
.ulist li button{{width:100%;text-align:left;background:none;border:1px solid transparent;border-radius:8px;padding:9px 10px;cursor:pointer;display:grid;grid-template-columns:auto 1fr auto;gap:2px 10px;align-items:center}}
.ulist li button:hover{{background:var(--sunk)}}
.ulist li button[aria-current="true"]{{background:var(--accent-soft);border-color:var(--accent)}}
.ulist .tab{{font-family:var(--mono);font-size:11.5px;color:var(--muted)}}
.ulist .pid{{font-family:var(--mono);font-size:12.5px;font-weight:500}}
.ulist .sub{{grid-column:2 / 4;font-size:12px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}

.dhead{{padding:18px 20px;display:grid;gap:10px}}
.dhead .row1{{display:flex;flex-wrap:wrap;align-items:center;gap:10px}}
.dhead h2{{font-family:var(--mono);font-size:19px;font-weight:500;letter-spacing:0}}
.callout{{border-radius:8px;padding:12px 14px;display:grid;gap:4px}}
.callout.bad{{background:var(--bad-bg);color:var(--bad)}} .callout.good{{background:var(--good-bg);color:var(--good)}}
.callout b{{font-size:15px}}
.callout span{{font-family:var(--mono);font-size:12.5px;color:var(--ink2)}}
.diag{{color:var(--ink2);max-width:120ch;margin:0}}
.facts{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;background:var(--line);border-top:1px solid var(--line)}}
.facts div{{background:var(--surface);padding:10px 14px;min-width:0}}
.facts small{{display:block;color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;font-weight:600}}
.facts span{{display:block;font-family:var(--mono);font-size:12.5px;overflow-wrap:anywhere}}
.facts .hl{{color:var(--accent);font-weight:500}}
.facts .unk{{color:var(--warn);font-weight:500}}

.stepper{{display:grid;grid-template-columns:repeat(9,minmax(0,1fr));gap:6px}}
.step{{border-radius:6px;padding:8px 8px 9px;background:var(--sunk);border:1px solid var(--line);display:grid;gap:3px;align-content:start;min-width:0}}
.step small{{font-family:var(--mono);font-size:10.5px;color:var(--muted)}}
.step b{{font-size:12px;line-height:1.25;font-weight:600}}
.step span{{font-family:var(--mono);font-size:11px;color:var(--muted)}}
.step.y{{background:var(--good-bg);border-color:transparent}} .step.y b{{color:var(--good)}}
.step.stop{{background:var(--bad-bg);border-color:var(--bad)}} .step.stop b{{color:var(--bad)}}
.step.n{{opacity:.6}}

.lane{{position:relative;margin-top:6px}}
.lanebar{{display:flex;height:34px;border-radius:6px;overflow:hidden;background:var(--sunk)}}
.lanebar div{{height:100%;min-width:3px;border-right:1px solid var(--surface);position:relative}}
.ticks{{position:relative;height:14px;margin-top:3px}}
.ticks i{{position:absolute;top:0;width:1px;height:9px;background:var(--ink2);opacity:.55}}
.ticks i.hi{{background:var(--bad);opacity:1;height:14px;width:2px}}
.axis{{display:flex;justify-content:space-between;font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:2px}}
.legend{{display:flex;flex-wrap:wrap;gap:8px 14px;font-size:12px;color:var(--ink2);margin-top:10px}}
.legend span{{display:inline-flex;align-items:center;gap:6px}}

.visits{{display:grid;gap:0}}
.visit{{display:grid;grid-template-columns:34px 170px 150px 1fr;gap:12px;padding:10px 0;border-bottom:1px solid var(--line);align-items:start}}
.visit:last-child{{border-bottom:0}}
.visit .num{{font-family:var(--mono);color:var(--muted);font-size:12px;padding-top:2px}}
.visit .when{{font-family:var(--mono);font-size:12px;color:var(--ink2)}}
.visit .when em{{display:block;font-style:normal;color:var(--muted)}}
.visit ul{{margin:4px 0 0;padding-left:16px;font-size:12.5px;color:var(--ink2)}}
.visit .cnt{{font-size:12.5px;color:var(--muted)}}
.exit{{margin-top:8px;padding:10px 14px;border-radius:8px;font-weight:600;font-size:13px}}
.exit.bad{{background:var(--bad);color:var(--surface)}} .exit.good{{background:var(--good);color:var(--surface)}}

.tl-ctrl{{display:flex;flex-wrap:wrap;gap:8px 14px;align-items:center;margin-bottom:10px}}
.tl-ctrl input[type=search]{{padding:6px 10px;border:1px solid var(--line2);border-radius:6px;background:var(--surface);min-width:220px;flex:1;max-width:340px}}
.tl-ctrl label{{display:inline-flex;gap:6px;align-items:center;font-size:12.5px;color:var(--ink2);cursor:pointer}}
.tl-ctrl select{{padding:6px 8px;border:1px solid var(--line2);border-radius:6px;background:var(--surface)}}
.tl td{{font-size:12.5px}}
.tl td.t{{font-family:var(--mono);white-space:nowrap}}
.tl td.t em{{display:block;font-style:normal;color:var(--muted);font-size:11px}}
.tl td.gap{{font-family:var(--mono);white-space:nowrap;text-align:right}}
.tl td.gap.idle{{color:var(--warn);font-weight:600}}
.tl td.gap.long{{color:var(--bad);font-weight:600}}
.tl .desc{{max-width:52ch}}
.tl .det{{color:var(--muted);font-size:12px;font-family:var(--mono);margin-top:2px;overflow-wrap:anywhere;max-width:70ch}}
.tl .flags{{display:flex;flex-wrap:wrap;gap:4px;max-width:360px}}
.flag{{font-size:11px;padding:1px 6px;border-radius:4px;background:var(--sunk);color:var(--ink2);border:1px solid var(--line)}}
.flag.r{{background:var(--bad-bg);color:var(--bad);border-color:transparent}}
.flag.w{{background:var(--warn-bg);color:var(--warn);border-color:transparent}}
.flag.b{{background:var(--accent-soft);color:var(--accent);border-color:transparent}}
tr.lastrow td{{background:var(--bad-bg)}}
tr.lastrow.good td{{background:var(--good-bg)}}
tr.sesbreak td{{border-top:2px dashed var(--line2)}}
.evt{{font-family:var(--mono);font-size:11.5px;font-weight:500}}
.evt.cancelled{{color:var(--bad)}} .evt.downloaded{{color:var(--accent)}} .evt.accepted,.evt.answered{{color:var(--good)}} .evt.installed{{color:var(--ink)}}
.count{{font-size:12px;color:var(--muted)}}

.legendbox{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px 24px;font-size:13px;color:var(--ink2)}}
.legendbox b{{color:var(--ink)}}

@media (max-width:1100px){{
  .kpis{{grid-template-columns:repeat(3,minmax(0,1fr))}}
  .grid2{{grid-template-columns:1fr}}
  .uview{{grid-template-columns:1fr}}
  .ulist{{position:static;max-height:340px}}
  .facts{{grid-template-columns:repeat(2,minmax(0,1fr))}}
  .stepper{{grid-template-columns:repeat(3,minmax(0,1fr))}}
}}
@media (max-width:640px){{
  .wrap{{padding-inline:16px}}
  .kpis{{grid-template-columns:repeat(2,minmax(0,1fr))}}
  .bar{{grid-template-columns:110px 1fr 44px;gap:8px}}
  .visit{{grid-template-columns:26px 1fr;}}
  .visit .when,.visit .cnt{{grid-column:2}}
  .issue dl{{grid-template-columns:1fr}}
  .facts{{grid-template-columns:1fr}}
}}
@media (prefers-reduced-motion:no-preference){{.fill{{transition:width .4s ease}}}}

/* day strip + cohorts */
.daybar{{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px;align-items:center}}
.daybar .lab{{font-size:12px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-right:4px}}
.daychip{{border:1px solid var(--line2);background:var(--surface);border-radius:10px;padding:6px 12px;cursor:pointer;display:grid;gap:0;text-align:left;min-width:92px}}
.daychip b{{font-size:13px}}
.daychip span{{font-size:11.5px;color:var(--muted)}}
.daychip.yest{{border-color:var(--accent)}}
.daychip.yest b::after{{content:" · yesterday";color:var(--accent);font-weight:600}}
.daychip[aria-pressed="true"]{{background:var(--ink);border-color:var(--ink)}}
.daychip[aria-pressed="true"] b,.daychip[aria-pressed="true"] span{{color:var(--ground)}}
.daychip[aria-pressed="true"].yest b::after{{color:var(--ground)}}
.cohorts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;align-items:start}}
.cohort{{padding:14px 16px;display:grid;gap:10px;align-content:start}}
.cohort.yest{{border:2px solid var(--accent)}}
.cohort .ch{{display:flex;justify-content:space-between;align-items:baseline;gap:8px}}
.cohort .ch b{{font-family:var(--cond);font-size:18px}}
.cohort .ch em{{font-style:normal;font-size:11.5px;font-weight:600;color:var(--accent);text-transform:uppercase;letter-spacing:.06em}}
.cohort .stat{{font-size:12.5px;color:var(--muted)}}
.cohort ul{{list-style:none;margin:0;padding:0;display:grid;gap:4px}}
.cohort li button{{width:100%;background:var(--sunk);border:1px solid transparent;border-radius:6px;padding:6px 8px;cursor:pointer;display:grid;grid-template-columns:auto 1fr;gap:0 8px;text-align:left;align-items:center}}
.cohort li button:hover{{border-color:var(--accent)}}
.cohort li .pid{{font-family:var(--mono);font-size:12px}}
.cohort li .why{{grid-column:2;font-size:11.5px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.cohort li.ret button{{background:none;border:1px dashed var(--line2)}}
.gh{{font-size:11px;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);font-weight:600;padding:10px 10px 4px}}
.facts-list{{margin:0;padding-left:18px;color:var(--ink2);display:grid;gap:4px;max-width:120ch}}
.lanebar .gapseg{{background:repeating-linear-gradient(135deg,var(--line2) 0 3px,transparent 3px 7px);min-width:14px;border-right:1px solid var(--surface);display:flex;align-items:center;justify-content:center}}
.gaplabels{{position:relative;height:16px;font-family:var(--mono);font-size:10.5px;color:var(--muted)}}
.gaplabels span{{position:absolute;transform:translateX(-50%);white-space:nowrap}}
tr.badrow td{{background:var(--warn-bg)}}
.loc{{font-size:12.5px}}
</style>
<header class="top">
  <div class="wrap">
    <div class="brand"><b>maternity FOUNDATION · Safe Delivery Flow Analysis</b><span id="meta"></span></div>
    <nav class="tabs" role="tablist" aria-label="Views">
      <button role="tab" id="t-overview" data-view="overview">Overview</button>
      <button role="tab" id="t-users" data-view="users">Users</button>
      <button role="tab" id="t-issues" data-view="issues">Issues</button>
      <button role="tab" id="t-events" data-view="events">All Events</button>
      <button role="tab" id="t-data" data-view="data">Data</button>
    </nav>
  </div>
</header>

<main class="wrap">
  <div class="daybar" id="daybar" aria-label="Filter by day users first appeared"></div>
  <section id="v-overview" data-v></section>
  <section id="v-users" data-v hidden></section>
  <section id="v-issues" data-v hidden></section>
  <section id="v-events" data-v hidden></section>
  <section id="v-data" data-v hidden></section>
</main>

<script>
const D = {data_json};
const ALLU = D.users;
const PH = {{splash:'Splash',language:'Language',category:'Categories',onboarding:'Onboarding',survey:'Survey',app:'In app'}};
const SCRL = D.screens;
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const dayLbl = d => {{ if(!d) return '—'; const parts=d.split('-'); if(parts.length<3) return d; return `${{+parts[2]}} ${{MON[+parts[1]-1]}}`; }};
function fdt(s, ms=true){{ if(!s) return '—'; const parts=s.split(' '); if(parts.length<2) return s; return `${{dayLbl(parts[0])}} ${{ms?parts[1]:parts[1].slice(0,8)}}`; }}
const ftime = s => s ? (s.split(' ')[1] || s) : '—';
function human(sec){{ if(sec==null || isNaN(sec)) return '—'; if(sec<60) return sec.toFixed(1)+'s'; const m=Math.floor(sec/60); if(m<60) return `${{m}}m ${{String(Math.floor(sec-m*60)).padStart(2,'0')}}s`; const h=Math.floor(m/60); if(h<24) return `${{h}}h ${{String(m%60).padStart(2,'0')}}m`; return `${{Math.floor(h/24)}}d ${{h%24}}h`; }}
const scrName = s => SCRL[s] || String(s||'').replace(/_/g,' ');
const phOf = s => ({{splash_screen:'splash',language_list_screen:'language',category_list_screen:'category',onboarding_screen:'onboarding',settings_disclaimer_screen:'onboarding',onboarding_survey_screen:'survey',onboarding_complete_screen:'survey'}})[s] || 'app';
const scrChip = (s) => {{ const p=phOf(s); return `<span class="scr" style="background:var(--s-${{p}}-bg);color:var(--ink)"><span class="dot" style="background:var(--s-${{p}})"></span>${{esc(scrName(s))}}</span>`; }};
const verPill = v => v==='Unknown' ? `<span class="pill ver unknown">version unknown</span>` : `<span class="pill ver">v${{esc(v)}}</span>`;
const outPill = o => `<span class="pill ${{o==='Completed'?'good':'bad'}}">${{o==='Completed'?'Reached Home':'Dropped'}}</span>`;
const byTab = Object.fromEntries(ALLU.map(u=>[u.tab,u]));
const byPid = Object.fromEntries(ALLU.map(u=>[u.pid,u]));
const short = p => p ? p.slice(4,15) : '';
function flagClass(f){{ if(/LAST EVENT|BACK to|cancelled .* after start|Duplicate|permission prompt/.test(f)) return 'r'; if(/Idle|Came back|New session|backgrounded|after launch|App launched|Installed/.test(f)) return 'w'; if(/^→/.test(f)) return 'b'; return ''; }}
const BP_ORDER = ['Splash','Language screen','Category screen','Onboarding assets','Survey','Completed'];
const BP_LABEL = {{'Splash':'Splash / launch','Language screen':'Language screen','Category screen':'Category screen','Onboarding assets':'Onboarding asset download','Survey':'Survey','Completed':'Completed → Home'}};

if(document.getElementById('meta')){{
  document.getElementById('meta').textContent = `Filtered: app v${{D.filter.version}} only · ${{ALLU.length}} users · ${{D.total}} events · ${{fdt(D.window[0],false)}} → ${{fdt(D.window[1],false)}} · times in IST (UTC+05:30) · export of ${{dayLbl(D.report_day)}}`;
}}

/* ---------- day filter ---------- */
let day='all';
const U = () => day==='all' ? ALLU : ALLU.filter(u=>u.cohort===day);
const returning = d => ALLU.filter(u=>u.cohort!==d && u.days && u.days.includes(d));
function renderDaybar(){{
  const chips = [`<span class="lab">First seen</span>`, `<button class="daychip" data-day="all" aria-pressed="${{day==='all'}}"><b>All days</b><span>${{ALLU.length}} users</span></button>`];
  [...(D.days||[])].reverse().forEach(d=>{{
    const n=ALLU.filter(u=>u.cohort===d).length, r=returning(d).length;
    chips.push(`<button class="daychip ${{d===D.yesterday?'yest':''}}" data-day="${{d}}" aria-pressed="${{day===d}}"><b>${{dayLbl(d)}}</b><span>${{n}} new${{r?` · ${{r}} returning`:''}}</span></button>`);
  }});
  const bar = document.getElementById('daybar');
  if(bar) bar.innerHTML = chips.join('');
}}

/* ---------- routing ---------- */
let view='overview', current=null;
function show(v, tab){{
  view=v;
  document.querySelectorAll('[data-v]').forEach(s=>s.hidden = s.id !== 'v-'+v);
  document.querySelectorAll('.tabs button').forEach(b=>b.setAttribute('aria-selected', b.dataset.view===v));
  const dbar = document.getElementById('daybar');
  if(dbar) dbar.hidden = (v==='issues'||v==='data');
  if(v==='users'){{ if(tab) current=tab; renderUsers(); }}
  if(v==='events') renderEvents();
  try{{ history.replaceState(null,'','#'+(v==='users'&&current?current:v)); }}catch(e){{}}
  window.scrollTo({{top:0}});
}}
function rerenderAll(){{ document.getElementById('v-overview').innerHTML=overview(); if(view==='users') renderUsers(); if(view==='events') renderEvents(); }}

/* ---------- overview ---------- */
function cohortCards(){{
  return [...(D.days||[])].reverse().map(d=>{{
    const nu=ALLU.filter(u=>u.cohort===d), ret=returning(d), done=nu.filter(u=>u.outcome==='Completed').length;
    if(!nu.length && !ret.length) return '';
    const y = d===D.yesterday;
    return `<article class="panel cohort ${{y?'yest':''}}">
      <div class="ch"><b>${{dayLbl(d)}}</b>${{y?'<em>Yesterday</em>':''}}</div>
      <div class="stat">${{nu.length}} new user${{nu.length!==1?'s':''}} · ${{done}} reached Home${{ret.length?` · ${{ret.length}} returning`:''}}</div>
      <ul>${{nu.map(u=>`<li><button data-user="${{u.tab}}"><span class="dot" style="background:var(--${{u.outcome==='Completed'?'good':'bad'}})"></span><span class="pid">${{u.tab}} ${{short(u.pid)}} <span style="color:var(--muted)">v${{esc(u.ver)}}</span></span><span class="why">${{esc(u.broke)}}</span></button></li>`).join('')}}
      ${{ret.map(u=>`<li class="ret"><button data-user="${{u.tab}}"><span class="dot" style="background:var(--muted)"></span><span class="pid">${{u.tab}} ${{short(u.pid)}}</span><span class="why">Returning (first seen ${{dayLbl(u.cohort)}})</span></button></li>`).join('')}}</ul>
    </article>`}}).join('');
}}
function overview(){{
  const us=U(), n=us.length; if(!n) return `<div class="sec panel pad"><p>No users on ${{dayLbl(day)}}.</p></div>`;
  const done=us.filter(u=>u.outcome==='Completed').length;
  const stageCounts=D.stages.map((_,i)=>us.filter(u=>u.stages && u.stages[i]).length);
  const bps=BP_ORDER.map(b=>us.filter(u=>u.bp===b));
  const vers={{}}; us.forEach(u=>{{(vers[u.ver]=vers[u.ver]||[]).push(u)}});
  const scope = day==='all' ? 'all users' : `users first seen ${{dayLbl(day)}}${{day===D.yesterday?' (yesterday)':''}}`;
  const p0=(D.issues||[]).filter(i=>i.sev==='P0');
  return `
  ${{day==='all'?`<div class="sec"><div class="sec-h"><h2>Users by the day they first appeared</h2><p>Newest first · pick a day above to filter</p></div><div class="cohorts">${{cohortCards()}}</div></div>`:''}}
  <div class="kpis">
    <div class="panel kpi"><small>Users</small><b>${{n}}</b><em>${{scope}}</em></div>
    <div class="panel kpi good"><small>Reached Home</small><b>${{done}}</b><em>completed onboarding</em></div>
    <div class="panel kpi bad"><small>Dropped</small><b>${{n-done}}</b><em>stopped before Home</em></div>
    <div class="panel kpi"><small>Completion</small><b>${{Math.round(done/n*100)}}%</b><em>${{done}} of ${{n}}</em></div>
    <div class="panel kpi"><small>App versions</small><b style="font-size:17px;font-family:var(--mono);line-height:1.6">${{Object.keys(vers).sort().map(v=>v==='Unknown'?'?':v).join(' · ')}}</b><em>${{Object.entries(vers).map(([v,a])=>`${{a.length}}× ${{v}}`).join(', ')}}</em></div>
  </div>
  <div class="grid2 sec">
    <div class="panel pad">
      <div class="sec-h"><h2>Onboarding funnel</h2><p>${{esc(scope)}} · red = lost at that step</p></div>
      <div class="bars">${{D.stages.map((s,i)=>{{ const c=stageCounts[i], lost=i>0?Math.max(0,stageCounts[i-1]-c):0;
        return `<div class="bar"><span class="lbl" title="${{esc(s)}}"><i>${{i+1}}</i>${{esc(s)}}</span><div class="track"><div class="fill" style="width:${{c/n*100}}%">${{c}}</div></div><span class="lost ${{lost>=2?'hot':''}}">${{lost?'−'+lost:''}}</span></div>`}}).join('')}}</div>
    </div>
    <div class="panel pad">
      <div class="sec-h"><h2>Where each user broke</h2><p>Furthest step reached</p></div>
      <div class="bars">${{BP_ORDER.map((b,i)=>`<div class="bar"><span class="lbl">${{esc(BP_LABEL[b]||b)}}</span><div class="track"><div class="fill" style="width:${{bps[i].length/n*100}}%;background:var(--${{b==='Completed'?'good':'bad'}})">${{bps[i].length||''}}</div></div><span class="lost">${{Math.round(bps[i].length/n*100)}}%</span></div>`).join('')}}</div>
    </div>
  </div>
  <div class="sec panel">
    <div class="pad" style="padding-bottom:6px"><div class="sec-h"><h2>Journey matrix</h2><p>Green = reached · red = last stage before dropping · click row for user</p></div></div>
    <div class="tblwrap"><table class="matrix"><thead><tr><th>User</th><th>First seen</th><th>Version</th>${{D.stages.map((s,i)=>`<th class="st">${{i+1}}. ${{esc(s)}}</th>`).join('')}}<th>Where it broke</th></tr></thead><tbody>
    ${{us.map(u=>`<tr class="click" data-user="${{u.tab}}" tabindex="0"><td style="white-space:nowrap"><span class="mono" style="color:var(--muted)">${{u.tab}}</span> <span class="mono">${{short(u.pid)}}</span>${{u.emu?' <span class="count">(emu)</span>':''}}</td><td class="mono" style="white-space:nowrap">${{dayLbl(u.cohort)}}</td><td>${{verPill(u.ver)}}</td>
      ${{u.stages.map((t,i)=>{{const stop=u.outcome!=='Completed'&&i===u.furthest-1; return `<td class="st"><span class="cell ${{t?(stop?'stop':'y'):'n'}}"></span></td>`}}).join('')}}
      <td style="min-width:240px;font-size:12.5px">${{esc(u.broke)}}</td></tr>`).join('')}}
    </tbody></table></div>
  </div>
  ${{p0.length?`<div class="sec"><div class="sec-h"><h2>Blocking problems (P0)</h2><p><a href="#issues" data-go="issues">All ${{D.issues.length}} issues →</a></p></div><div class="issues">${{p0.map(issueCard).join('')}}</div></div>`:''}}`;
}}
function issueCard(i){{
  const links = (i.users&&i.users.length) ? `<div class="ulinks">${{i.users.map(p=>byPid[p]?`<button class="ulink" data-user="${{byPid[p].tab}}">${{byPid[p].tab}} ${{short(p)}}</button>`:p).join('')}}</div>` : '—';
  return `<article class="panel issue ${{i.sev}}"><div class="stripe"></div><div class="body"><div class="head"><span class="sev ${{i.sev}}">${{i.sev}}</span><span class="area">${{esc(i.area)}}</span></div>
    <div class="finding">${{esc(i.finding)}}</div><dl><dt>Evidence</dt><dd>${{esc(i.evidence)}}</dd><dt>Users</dt><dd>${{links}}</dd><dt>Fix / check</dt><dd>${{esc(i.fix)}}</dd></dl></div></article>`;
}}

/* ---------- users view ---------- */
let uq='', uf='all', tl={{q:'',flag:false,noise:false,ph:'all'}};
function renderUsers(){{
  const h=document.getElementById('v-users');
  if(!h.dataset.built){{
    h.innerHTML=`<div class="uview"><aside class="panel ulist" aria-label="Users"><div class="ctrl"><input id="uq" type="search" placeholder="Search profile, device…" aria-label="Search users">
      <div class="filters" id="uf"><button class="chipbtn" data-f="all" aria-pressed="true">All</button><button class="chipbtn" data-f="Dropped" aria-pressed="false">Dropped</button><button class="chipbtn" data-f="Completed" aria-pressed="false">Reached Home</button></div></div><ul id="ul"></ul></aside><div id="ud"></div></div>`;
    h.dataset.built=1;
    document.getElementById('uq').addEventListener('input',e=>{{uq=e.target.value; renderList();}});
    document.getElementById('uf').addEventListener('click',e=>{{const b=e.target.closest('[data-f]'); if(!b) return; uf=b.dataset.f; document.querySelectorAll('#uf .chipbtn').forEach(x=>x.setAttribute('aria-pressed',x===b)); renderList();}});
  }}
  const us=U(); if(!current || !us.find(u=>u.tab===current)) current = (us.find(u=>u.outcome==='Dropped')||us[0]||ALLU[0]||{{tab:'U01'}}).tab;
  renderList(); if(byTab[current]) renderDetail(byTab[current]);
}}
function renderList(){{
  const q=uq.toLowerCase();
  const us=U().filter(u=>(uf==='all'||u.outcome===uf)&&(!q||[u.pid,u.device,u.ver,u.broke,u.tab].join(' ').toLowerCase().includes(q)));
  const groups={{}}; us.forEach(u=>(groups[u.cohort]=groups[u.cohort]||[]).push(u));
  document.getElementById('ul').innerHTML = Object.keys(groups).sort().reverse().map(d=>`<li class="gh">${{dayLbl(d)}} · ${{groups[d].length}}</li>`+groups[d].map(u=>`<li><button data-user="${{u.tab}}" aria-current="${{u.tab===current}}"><span class="tab">${{u.tab}}</span><span class="pid">${{short(u.pid)}}</span><span class="dot" style="background:var(--${{u.outcome==='Completed'?'good':'bad'}})"></span><span class="sub">v${{esc(u.ver)}} · ${{esc(u.broke)}}</span></button></li>`).join('')).join('') || '<li class="count" style="padding:12px">No users match.</li>';
}}
function renderDetail(u){{
  const good=u.outcome==='Completed'; const stop=good?-1:u.furthest-1;
  const loc = u.location ? `${{esc(u.location.area)}} <span class="count">(approx. ${{u.location.lat}}, ${{u.location.lon}})</span>` : '<span class="count">Not recorded</span>';
  document.getElementById('ud').innerHTML = `
  <div class="panel"><div class="dhead">
    <div class="row1"><span class="mono" style="color:var(--muted)">${{u.tab}}</span><h2>${{esc(u.pid)}}</h2>${{outPill(u.outcome)}}${{verPill(u.ver)}}<span class="pill neutral">${{esc(u.device)}}${{u.emu?' · emulator':''}}</span></div>
    <div class="callout ${{good?'good':'bad'}}"><b>${{esc(u.broke)}}</b><span>${{esc(u.exit_txt)}} · ${{fdt(u.last)}} IST</span></div>
    ${{u.key?`<p class="diag" style="color:var(--${{good?'good':'bad'}});font-weight:600;margin:0">Key finding: ${{esc(u.key)}}</p>`:''}}
    <ul class="facts-list">${{(u.facts||[]).map(f=>`<li>${{esc(f)}}</li>`).join('')}}</ul>
  </div>
  <div class="facts">
    <div><small>App version</small><span class="hl">v${{esc(u.ver)}}</span></div>
    <div><small>Device / OS</small><span>${{esc(u.device)}} · ${{esc(u.os)}}</span></div>
    <div><small>Location</small><span class="loc">${{loc}}</span></div>
    <div><small>Content language</small><span>${{esc(u.lang||'Not chosen')}}</span></div>
    <div><small>First seen</small><span>${{fdt(u.first)}}</span></div>
    <div><small>Last seen</small><span>${{fdt(u.last)}}</span></div>
    <div><small>Total duration</small><span>${{u.span}}</span></div>
    <div><small>Total events</small><span>${{u.n}}</span></div>
  </div></div>

  <div class="panel pad sec"><div class="sec-h"><h2>Journey checklist</h2><p>Funnel progression for this user</p></div>
    <div class="stepper" style="grid-template-columns:repeat(auto-fill,minmax(112px,1fr))">${{D.stages.map((s,i)=>{{const t=u.stages[i]; const cls=t?(i===stop?'stop':'y'):'n';
      return `<div class="step ${{cls}}"><small>${{i+1}}${{i===stop?' · last reached':''}}</small><b>${{esc(s)}}</b><span>${{t?fdt(t,false):'not reached'}}</span></div>`}}).join('')}}</div></div>

  <div class="panel pad sec"><div class="sec-h"><h2>Full event timeline</h2><p class="count" id="tlcount"></p></div>
    <div class="tl-ctrl"><input type="search" id="tlq" placeholder="Filter events…" aria-label="Filter events" value="${{esc(tl.q)}}">
      <select id="tlp"><option value="all">All phases</option>${{Object.keys(PH).map(p=>`<option value="${{p}}" ${{tl.ph===p?'selected':''}}>${{PH[p]}}</option>`).join('')}}</select>
      <label><input type="checkbox" id="tlf" ${{tl.flag?'checked':''}}> Flagged only</label></div>
    <div class="tblwrap"><table class="tl"><thead><tr><th>#</th><th>Time (IST)</th><th style="text-align:right">Gap</th><th>Elapsed</th><th>Screen</th><th>Event</th><th>What happened</th><th>Debug flags</th></tr></thead><tbody id="tlb"></tbody></table></div></div>`;
  const re=()=>renderTL(u);
  document.getElementById('tlq').addEventListener('input',e=>{{tl.q=e.target.value;re();}});
  document.getElementById('tlp').addEventListener('change',e=>{{tl.ph=e.target.value;re();}});
  document.getElementById('tlf').addEventListener('change',e=>{{tl.flag=e.target.checked;re();}});
  renderTL(u);
}}
function renderTL(u){{
  const q=tl.q.toLowerCase(), N=(u.events||[]).length, good=u.outcome==='Completed';
  const rows=(u.events||[]).filter(e=>(tl.ph==='all'||e.ph===tl.ph)&&(!tl.flag||(e.flags&&e.flags.length))&&(!q||[e.desc,e.det,e.type,e.lang,(e.flags||[]).join(' ')].join(' ').toLowerCase().includes(q)));
  document.getElementById('tlcount').textContent=`Showing ${{rows.length}} of ${{N}} events`;
  document.getElementById('tlb').innerHTML=rows.map(e=>`<tr class="${{e.n===N?'lastrow':''}} ${{e.n===N&&good?'good':''}}">
    <td class="mono">${{e.n}}</td><td class="t">${{ftime(e.ist)}}<em>${{fdt(e.ist,false).split(' ').slice(0,2).join(' ')}}</em></td>
    <td class="gap">${{e.gap==null?'start':human(e.gap)}}</td><td class="mono">+${{human(e.el)}}</td><td>${{scrChip(e.scr)}}</td>
    <td><span class="evt ${{e.type}}">${{esc(e.type)}}</span></td>
    <td><div class="desc">${{esc(e.desc)}}</div>${{e.det?`<div class="det">${{esc(e.det)}}</div>`:''}}</td>
    <td><div class="flags">${{(e.flags||[]).map(f=>`<span class="flag ${{flagClass(f)}}">${{esc(f)}}</span>`).join('')}}</div></td></tr>`).join('')||'<tr><td colspan="8" class="count">No events match these filters.</td></tr>';
}}

/* ---------- issues view ---------- */
let sev='all';
function renderIssues(){{
  const sevs=['all','P0','P1','P2','Info'];
  document.getElementById('v-issues').innerHTML=`<div class="sec"><div class="sec-h"><h2>Bugs and telemetry gaps</h2><div class="filters" id="sf">${{sevs.map(s=>`<button class="chipbtn" data-s="${{s}}" aria-pressed="${{s===sev}}">${{s==='all'?'All':s}} ${{s==='all'?(D.issues||[]).length:(D.issues||[]).filter(i=>i.sev===s).length}}</button>`).join('')}}</div></div>
  <div class="issues">${{(D.issues||[]).filter(i=>sev==='all'||i.sev===sev).map(issueCard).join('')}}</div></div>`;
  document.getElementById('sf').addEventListener('click',e=>{{const b=e.target.closest('[data-s]'); if(b){{sev=b.dataset.s; renderIssues();}}}});
}}

/* ---------- all events ---------- */
let ef={{u:'all',p:'all',t:'all',q:''}};
function renderEvents(){{
  const h=document.getElementById('v-events'); const us=U(); const set=new Set(us.map(u=>u.tab));
  const ALL=us.flatMap(u=>(u.events||[]).map(e=>({{...e,u}})));
  h.innerHTML=`<div class="sec panel pad"><div class="sec-h"><h2>All events</h2><p class="count" id="evc"></p></div>
    <div class="tl-ctrl"><input type="search" id="evq" placeholder="Search description, flags…" value="${{esc(ef.q)}}">
      <select id="evu"><option value="all">All users</option>${{us.map(u=>`<option value="${{u.tab}}" ${{ef.u===u.tab?'selected':''}}>${{u.tab}} ${{short(u.pid)}}</option>`).join('')}}</select>
      <select id="evp"><option value="all">All phases</option>${{Object.keys(PH).map(p=>`<option value="${{p}}" ${{ef.p===p?'selected':''}}>${{PH[p]}}</option>`).join('')}}</select></div>
    <div class="tblwrap"><table class="tl"><thead><tr><th>User</th><th>Ver</th><th>#</th><th>Time (IST)</th><th>Screen</th><th>Event</th><th>What happened</th></tr></thead><tbody id="evb"></tbody></table></div></div>`;
  const draw=()=>{{ const q=ef.q.toLowerCase();
    const rows=ALL.filter(e=>(ef.u==='all'||e.u.tab===ef.u)&&(ef.p==='all'||e.ph===ef.p)&&(!q||[e.desc,e.det,e.u.pid].join(' ').toLowerCase().includes(q)));
    document.getElementById('evc').textContent=`${{rows.length}} of ${{ALL.length}} events`;
    document.getElementById('evb').innerHTML=rows.map(e=>`<tr><td><button class="ulink" data-user="${{e.u.tab}}">${{e.u.tab}} ${{short(e.u.pid)}}</button></td><td class="mono">${{esc(e.u.ver)}}</td><td class="mono">${{e.n}}</td><td class="t">${{fdt(e.ist)}}</td><td>${{scrChip(e.scr)}}</td><td><span class="evt ${{e.type}}">${{e.type}}</span></td><td><div class="desc">${{esc(e.desc)}}</div></td></tr>`).join(''); }};
  document.getElementById('evq').addEventListener('input',e=>{{ef.q=e.target.value; draw();}});
  document.getElementById('evu').addEventListener('change',e=>{{ef.u=e.target.value; draw();}});
  document.getElementById('evp').addEventListener('change',e=>{{ef.p=e.target.value; draw();}});
  draw();
}}

/* ---------- data ---------- */
function renderData(){{
  document.getElementById('v-data').innerHTML=`<div class="grid2 sec"><div class="panel pad"><div class="sec-h"><h2>Export Details</h2><p>${{D.total}} events</p></div><p>Processed from PostgreSQL production tables.</p></div></div>`;
}}

document.addEventListener('DOMContentLoaded', ()=>{{
  renderDaybar();
  document.getElementById('v-overview').innerHTML=overview();
  renderIssues();
  renderData();
}});

document.querySelectorAll('.tabs button').forEach(b=>b.addEventListener('click',()=>show(b.dataset.view)));
document.addEventListener('click',e=>{{ const t=e.target.closest('[data-user]'); if(t){{ e.preventDefault(); show('users', t.dataset.user);}} const g=e.target.closest('[data-go]'); if(g){{ e.preventDefault(); show(g.dataset.go);}} }});
const daybarEl = document.getElementById('daybar');
if(daybarEl) daybarEl.addEventListener('click',e=>{{const b=e.target.closest('[data-day]'); if(!b) return; day=b.dataset.day; renderDaybar(); rerenderAll();}});
renderDaybar();
document.getElementById('v-overview').innerHTML=overview();
renderIssues();
renderData();
</script>
</body></html>"""
        return html


def parse_journey_query(query: str, available_dates: list = None):
    q = query.lower()
    journey_keywords = [
        "user journey", "journey", "onboarding flow", "footfall",
        "drop off", "drop-off", "dropoff", "report of all the users",
        "report of users", "report for users", "flow debugger",
        "where did users", "where do users", "user report"
    ]
    is_journey = any(k in q for k in journey_keywords)
    if not is_journey and ("report" in q and ("user" in q or "date" in q)):
        is_journey = True

    # Check for specific profile ID format
    pid_match = re.search(r'\b[A-Z0-9]{3}-[A-Z0-9]{3}-[A-Z0-9]{3}-[A-Z0-9]{3}-[A-Z0-9]{2}\b', query, re.IGNORECASE)
    pid = pid_match.group(0).upper() if pid_match else None

    # Check for date in format YYYY-MM-DD
    date_match = re.search(r'\b\d{4}-\d{2}-\d{2}\b', query)
    date_val = None
    if date_match:
        date_val = date_match.group(0)
    elif "yesterday" in q:
        date_val = "yesterday"
    elif "today" in q and available_dates:
        date_val = available_dates[0]
    elif is_journey and not date_val:
        # Default to latest active date if available
        date_val = available_dates[0] if available_dates else "all"

    return is_journey, date_val, pid


