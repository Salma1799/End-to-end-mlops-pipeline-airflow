"""Dashboard TaxiFlow (Streamlit) — tableau de bord BI du pipeline orchestré par Airflow.

Le dashboard ne calcule rien lui-même : il lit les rapports JSON que chaque tâche du DAG
écrit dans le dossier reports/ (ingestion, qualité, entraînement, déploiement, Gold, dérive, KPIs).
Chaque nouveau run Airflow met donc le dashboard à jour automatiquement.

Style : tableau de bord BI sombre (tuiles, grands chiffres, graphiques simples, tableaux colorés).
Lancement : service "dashboard" du docker-compose -> http://localhost:8501
"""
import glob
import json
import os
import re

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

REPORTS_DIR = os.getenv("REPORTS_DIR", "/app/reports")

# Palette BI (une couleur principale + couleurs de catégories)
CYAN = "#1FA8C9"
NAVY = "#454E7C"
GREEN = "#5AC189"
ORANGE = "#FF7F44"
RED = "#E04355"
YELLOW = "#FCC700"
GREY = "#8C8C8C"
PALETTE = [CYAN, ORANGE, NAVY, GREEN, YELLOW, "#A868B7", GREY]
TEXT = "#E8E8E8"
MUTED = "#9A9A9A"
GRID = "#2C2C2C"

st.set_page_config(page_title="TaxiFlow · Dashboard", layout="wide", initial_sidebar_state="collapsed")

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
html, body, [class*="css"], .stApp {{ font-family: 'Inter', 'Segoe UI', sans-serif; }}
.stApp {{ background: #141414; }}
header[data-testid="stHeader"] {{ background: #141414; height: 0; }}
section[data-testid="stSidebar"] {{ display: none; }}
.block-container {{ padding-top: 1.2rem; padding-bottom: 1rem; max-width: 100%; }}
/* Tuiles */
div[data-testid="stVerticalBlockBorderWrapper"]:has(> div > div[data-testid="stVerticalBlock"] > div[data-testid="stElementContainer"] > div[data-testid="stMarkdown"] .ptitle) {{
  background: #1E1E1E; border: 1px solid #2C2C2C !important; border-radius: 4px;
}}
.ptitle {{ color: {TEXT}; font-size: 14px; font-weight: 600; margin: 0 0 4px 2px; }}
.psub {{ color: {MUTED}; font-size: 12px; margin: -2px 0 6px 2px; }}
.bignum {{ color: {TEXT}; font-size: 64px; font-weight: 300; line-height: 1.05; margin: 18px 0 6px 6px; }}
.bignum.txt {{ font-size: 40px; margin-top: 26px; }}
.bignum small {{ font-size: 26px; color: {MUTED}; margin-left: 6px; }}
.bigsub {{ color: {MUTED}; font-size: 13px; margin: 0 0 10px 8px; }}
.ok {{ color: {GREEN}; }} .ko {{ color: {RED}; }}
/* En-tête */
.dhead {{ display:flex; align-items:center; gap:12px; margin-bottom: 2px; }}
.dtitle {{ color:{TEXT}; font-size: 22px; font-weight: 600; }}
.chip {{ font-size: 11px; color: {CYAN}; border: 1px solid {CYAN}; border-radius: 3px; padding: 1px 6px; }}
.meta {{ color:{MUTED}; font-size: 12px; }}
/* Onglets */
.stTabs [data-baseweb="tab-list"] {{ gap: 22px; border-bottom: 1px solid #2C2C2C; }}
.stTabs [data-baseweb="tab"] {{ color: {MUTED}; font-size: 14px; padding: 6px 2px; background: transparent; }}
.stTabs [aria-selected="true"] {{ color: {TEXT} !important; }}
.stTabs [data-baseweb="tab-highlight"] {{ background-color: {CYAN}; }}
</style>
""", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- lecture des rapports
@st.cache_data(ttl=30)
def load_reports() -> dict:
    """{type: {mois: contenu}} pour tous les fichiers reports/<type>_<AAAA-MM>.json."""
    reports = {}
    for path in glob.glob(os.path.join(REPORTS_DIR, "*.json")):
        match = re.match(r"^([a-z]+)_(\d{4}-\d{2})\.json$", os.path.basename(path))
        if not match:
            continue
        kind, month = match.groups()
        try:
            with open(path, encoding="utf-8") as f:
                reports.setdefault(kind, {})[month] = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
    return reports


def pct(value) -> float:
    found = re.search(r"[\d.]+", str(value))
    return float(found.group()) if found else 0.0


def fmt(n) -> str:
    if isinstance(n, (int, float)):
        return f"{n:,.0f}".replace(",", " ")
    return "—"


def short(n) -> str:
    """3 500 000 -> 3.5M ; 114 818 -> 115k."""
    if not isinstance(n, (int, float)):
        return "—"
    if n >= 1e6:
        return f"{n / 1e6:.1f}M"
    if n >= 1e3:
        return f"{n / 1e3:.0f}k"
    return f"{n:.0f}"


# ----------------------------------------------------------------------------- briques visuelles
def chart(fig, height=300, legend=None):
    fig.update_layout(
        height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, Segoe UI, sans-serif", color=MUTED, size=12), colorway=PALETTE,
        margin=dict(l=10, r=40, t=24, b=10), separators=", ",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11),
                    title_text=""),
        hoverlabel=dict(bgcolor="#2A2A2A", font_color=TEXT),
    )
    if legend:
        fig.update_layout(legend=legend)
    fig.update_xaxes(showgrid=False, linecolor=GRID, tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED))
    fig.update_traces(cliponaxis=False, selector=dict(type="bar"))
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def title(text, sub=None):
    st.markdown(f"<div class='ptitle'>{text}</div>" + (f"<div class='psub'>{sub}</div>" if sub else ""),
                unsafe_allow_html=True)


def big_number(label, value, unit="", sub="", text=False):
    with st.container(border=True):
        title(label)
        cls = "bignum txt" if text else "bignum"
        st.markdown(f"<div class='{cls}'>{value}<small>{unit}</small></div><div class='bigsub'>{sub}</div>",
                    unsafe_allow_html=True)


def table(df, styler=None, height=None):
    data = styler(df.style) if styler else df
    st.dataframe(data, hide_index=True, use_container_width=True, height=height)


def cell(color):
    return f"background-color: {color}; color: #FFFFFF;"


def empty(msg):
    st.markdown(f"<div class='psub' style='padding:30px 4px'>{msg}</div>", unsafe_allow_html=True)


# ----------------------------------------------------------------------------- données
reports = load_reports()
months = sorted({m for kind in reports.values() for m in kind})

head_left, head_right = st.columns([5, 1])
with head_left:
    st.markdown("<div class='dhead'><span class='dtitle'>Dashboard TaxiFlow</span>"
                "<span class='chip'>Pipeline Airflow</span></div>"
                "<div class='meta'>Durée des courses de taxi à New York · alimenté par le DAG "
                "<b>taxiflow_pipeline</b></div>", unsafe_allow_html=True)

if not months:
    st.info(f"Aucun rapport dans {REPORTS_DIR}. Lance le DAG dans Airflow (http://localhost:8080), "
            "puis actualise la page.")
    st.stop()

with head_right:
    complete = sorted(reports.get("kpis", {})) or sorted(reports.get("gold", {}))
    default = complete[-1] if complete else months[-1]
    month = st.selectbox("Mois", months[::-1], index=months[::-1].index(default), label_visibility="collapsed")
    if st.button("Actualiser", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

get = lambda kind, m=month: reports.get(kind, {}).get(m)  # noqa: E731
ingestion, quality, features = get("ingestion"), get("quality"), get("features")
training, deployment, gold = get("training"), get("deployment"), get("gold")
drift, kpis = get("drift"), get("kpis")

champion = None
for m in sorted(reports.get("deployment", {})):
    if reports["deployment"][m].get("decision") == "promote":
        champion = reports["deployment"][m].get("challenger_version")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Vue d'ensemble", "Qualité des données", "Modèles", "Prédictions", "Dérive des données"])

# ============================================================================= 1. VUE D'ENSEMBLE
with tab1:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        big_number("Courses du mois (source NYC TLC)", short(ingestion["rows_raw"]) if ingestion else "—",
                   sub=f"{fmt(ingestion['rows_bronze'])} ingérées en Bronze" if ingestion else "")
    with c2:
        if quality:
            state = "<span class='ok'>accepté</span>" if quality["success"] else "<span class='ko'>bloqué</span>"
            big_number("Courses valides (Great Expectations)", f"{quality['valid_percent']:.1f}", "%",
                       sub=f"{quality['rules_passed']}/{quality['rules_total']} règles · mois {state}")
        else:
            big_number("Courses valides (Great Expectations)", "—")
    with c3:
        mae = training["models"][training["best_model"]]["mae"] if training else None
        big_number("Erreur moyenne du modèle (MAE)", f"{mae:.2f}" if mae else "—", " min",
                   sub="sur le jeu de test du mois")
    with c4:
        big_number("Modèle en production", f"v{champion}" if champion else "—",
                   sub="alias @champion · MLflow Registry")

    left, mid, right = st.columns([2, 1.2, 1.3])
    with left, st.container(border=True):
        title("Courses et durée moyenne par heure de départ")
        if kpis:
            h = pd.DataFrame(kpis["by_hour"])
            fig = go.Figure()
            fig.add_bar(x=h["pickup_hour"], y=h["trips"], name="Courses", marker_color=NAVY)
            fig.add_scatter(x=h["pickup_hour"], y=h["avg_real_min"], name="Durée moyenne (min)", yaxis="y2",
                            mode="lines+markers", line=dict(color=CYAN, width=2), marker=dict(size=5))
            fig.update_layout(xaxis=dict(dtick=2, title="Heure"),
                              yaxis2=dict(overlaying="y", side="right", showgrid=False, tickfont=dict(color=MUTED)))
            chart(fig, 300)
        else:
            empty("Pas encore de KPIs pour ce mois (tâche publish_kpis).")
    with mid, st.container(border=True):
        title("Départs par quartier")
        if kpis:
            b = pd.DataFrame(kpis["by_borough"])
            # Zones inconnues retirées, petits quartiers (< 3 %) regroupés dans "Autres"
            b = b[b["pickup_borough"].notna() & ~b["pickup_borough"].isin(["Unknown", "N/A", "NV"])]
            b = b.groupby("pickup_borough", as_index=False)["trips"].sum()
            b["share"] = b["trips"] / b["trips"].sum()
            small = b["share"] < 0.03
            if small.sum() > 1:
                b = pd.concat([b[~small], pd.DataFrame([{"pickup_borough": "Autres",
                                                         "trips": b.loc[small, "trips"].sum(),
                                                         "share": b.loc[small, "share"].sum()}])])
            b = b.sort_values("trips", ascending=False)
            colors = [GREY if n == "Autres" else c for n, c in zip(b["pickup_borough"], PALETTE)]
            fig = go.Figure(go.Pie(
                labels=b["pickup_borough"], values=b["trips"], hole=0.6, sort=False, direction="clockwise",
                marker=dict(colors=colors, line=dict(color="#1E1E1E", width=2)),
                text=[f"{s:.0%}" if s >= 0.05 else "" for s in b["share"]],
                textinfo="text", textposition="inside", insidetextorientation="horizontal",
                textfont=dict(color="#FFFFFF", size=13),
                hovertemplate="%{label} : %{value:,} courses (%{percent})<extra></extra>"))
            fig.update_traces(labels=[f"{n}  {s:.1%}" for n, s in zip(b["pickup_borough"], b["share"])])
            fig.update_layout(uniformtext=dict(minsize=11, mode="hide"))
            chart(fig, 320, legend=dict(orientation="v", x=1.0, xanchor="left", y=0.5, yanchor="middle",
                                        font=dict(size=12, color=TEXT)))
        else:
            empty("—")
    with right, st.container(border=True):
        title("Courses par jour de la semaine")
        if kpis:
            d = pd.DataFrame(kpis["by_day"])
            fig = go.Figure(go.Bar(x=d["day_name"], y=d["trips"], marker_color=CYAN))
            chart(fig, 300)
        else:
            empty("—")

    with st.container(border=True):
        title("Historique des runs du DAG", "Une ligne par mois traité par Airflow")
        rows = []
        for m in months:
            q, t, dp, dr = (reports.get(k, {}).get(m) for k in ("quality", "training", "deployment", "drift"))
            ing = reports.get("ingestion", {}).get(m)
            rows.append({
                "Mois": m,
                "Courses brutes": fmt(ing["rows_raw"]) if ing else "—",
                "Qualité": ("Accepté" if q["success"] else "Bloqué") if q else "—",
                "Valides (%)": f"{q['valid_percent']:.2f}" if q else "—",
                "Meilleur modèle": ("GBT" if t["best_model"] == "gbt" else "Régression linéaire") if t else "—",
                "MAE (min)": f"{t['models'][t['best_model']]['mae']:.2f}" if t else "—",
                "Décision": {"promote": "Promu", "keep": "Champion gardé"}.get(dp["decision"]) if dp else "—",
                "Dérive": {"ok": "OK", "alert": "Alerte", "no_reference": "Référence"}.get(dr["status"]) if dr else "—",
            })
        df = pd.DataFrame(rows)

        def color_runs(s):
            s = s.map(lambda v: cell(GREEN) if v in ("Accepté", "Promu", "OK")
                      else cell(RED) if v in ("Bloqué", "Alerte") else "", subset=["Qualité", "Décision", "Dérive"])
            return s
        table(df, color_runs)

# ============================================================================= 2. QUALITÉ
with tab2:
    if not quality:
        empty("Pas de rapport qualité pour ce mois.")
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            big_number("Courses valides", f"{quality['valid_percent']:.1f}", "%",
                       sub="seuil d'acceptation défini dans Great Expectations")
        with c2:
            big_number("Règles respectées", f"{quality['rules_passed']}", f" / {quality['rules_total']}",
                       sub="contrôles exécutés par la tâche validate_raw")
        with c3:
            big_number("Courses contrôlées", short(quality["rows_checked"]),
                       sub=("<span class='ok'>Mois accepté : le pipeline continue</span>" if quality["success"]
                            else "<span class='ko'>Mois bloqué : DAG arrêté (upstream_failed)</span>"))

        left, right = st.columns([1.6, 1])
        rules = pd.DataFrame(quality["rules"])
        rules["Erreurs (%)"] = rules["unexpected_percent"].map(pct)
        with left, st.container(border=True):
            title("Taux d'erreur par règle", "En rouge : règle non respectée")
            measured = rules[rules["unexpected_percent"] != "-"].sort_values("Erreurs (%)")
            fig = go.Figure(go.Bar(x=measured["Erreurs (%)"], y=measured["rule"], orientation="h",
                                   marker_color=[CYAN if ok else RED for ok in measured["success"]],
                                   text=[f"{v:.2f} %" for v in measured["Erreurs (%)"]], textposition="outside",
                                   textfont=dict(color=MUTED)))
            fig.update_layout(xaxis_title="% de courses en erreur")
            chart(fig, 420)
        with right, st.container(border=True):
            title("Courses valides par mois")
            q_all = sorted(reports.get("quality", {}).items())
            fig = go.Figure(go.Bar(x=[m for m, _ in q_all], y=[q["valid_percent"] for _, q in q_all],
                                   marker_color=[GREEN if q["success"] else RED for _, q in q_all],
                                   text=[f"{q['valid_percent']:.1f} %" for _, q in q_all], textposition="outside"))
            fig.update_xaxes(type="category")
            fig.update_yaxes(range=[0, 110])
            chart(fig, 420)

        with st.container(border=True):
            title("Détail des 18 règles Great Expectations")
            detail = rules[["rule", "success", "unexpected_percent", "tolerance"]].rename(columns={
                "rule": "Règle", "success": "Résultat", "unexpected_percent": "Erreurs", "tolerance": "Tolérance"})
            detail["Résultat"] = detail["Résultat"].map({True: "OK", False: "KO"})
            table(detail, lambda s: s.map(lambda v: cell(GREEN) if v == "OK" else cell(RED) if v == "KO" else "",
                                          subset=["Résultat"]))

# ============================================================================= 3. MODÈLES
with tab3:
    trained = reports.get("training", {})
    if not trained:
        empty("Aucun entraînement pour l'instant.")
    else:
        if training:
            best = training["models"][training["best_model"]]
            name = "GBT" if training["best_model"] == "gbt" else "Régression linéaire"
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                big_number("Meilleur modèle du mois", name, text=True, sub=f"enregistré en version {training['registered_version']}")
            with c2:
                big_number("MAE", f"{best['mae']:.2f}", " min", sub="erreur moyenne absolue")
            with c3:
                big_number("RMSE", f"{best['rmse']:.2f}", " min", sub="pénalise les grosses erreurs")
            with c4:
                big_number("R²", f"{best['r2']:.2f}", sub="part de la variation expliquée")

        hist = pd.DataFrame([{"Mois": m, "Modèle": "GBT" if k == "gbt" else "Régression linéaire",
                              "MAE": v["mae"], "R2": v["r2"]}
                             for m, t in sorted(trained.items()) for k, v in t["models"].items()])
        left, right = st.columns(2)
        for col, metric, label, rng in ((left, "MAE", "Erreur moyenne MAE (min) — plus bas = meilleur", None),
                                        (right, "R2", "R² — plus haut = meilleur", [0, 1])):
            with col, st.container(border=True):
                title(label, "Régression linéaire vs GBT (Spark MLlib)")
                fig = go.Figure()
                for model, color in (("Régression linéaire", GREY), ("GBT", CYAN)):
                    part = hist[hist["Modèle"] == model]
                    fig.add_bar(x=part["Mois"], y=part[metric], name=model, marker_color=color,
                                text=[f"{v:.2f}" for v in part[metric]], textposition="outside")
                fig.update_layout(barmode="group")
                fig.update_xaxes(type="category")
                if rng:
                    fig.update_yaxes(range=rng)
                chart(fig, 300)

        with st.container(border=True):
            title("Décisions de déploiement (champion / challenger)", "Résultat de la tâche decide_deploy")
            dec = pd.DataFrame([{"Mois": m, "Challenger": f"v{d['challenger_version']}",
                                 "Champion avant": f"v{d['champion_version']}" if d.get("champion_version") else "—",
                                 "Décision": "promote_model" if d["decision"] == "promote" else "keep_champion",
                                 "Raison": d["reason"]}
                                for m, d in sorted(reports.get("deployment", {}).items())])
            if len(dec):
                table(dec, lambda s: s.map(lambda v: cell(GREEN) if v == "promote_model"
                                           else cell(NAVY) if v == "keep_champion" else "", subset=["Décision"]))

# ============================================================================= 4. PRÉDICTIONS (GOLD)
with tab4:
    if not kpis:
        empty("Pas de KPIs pour ce mois : ils sont produits par la tâche publish_kpis à la fin du DAG.")
    else:
        t = kpis["totals"]
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            big_number("Courses prédites (Gold)", short(t["trips"]), sub=f"{fmt(t['trips'])} prédictions")
        with c2:
            big_number("Durée réelle moyenne", f"{t['avg_real_min']:.1f}", " min")
        with c3:
            big_number("Durée prédite moyenne", f"{t['avg_predicted_min']:.1f}", " min")
        with c4:
            big_number("Erreur moyenne (MAE)", f"{t['mae_min']:.2f}", " min", sub=f"modèle v{t.get('model_version', '—')}")

        with st.container(border=True):
            title("Durée réelle vs durée prédite selon l'heure de départ")
            h = pd.DataFrame(kpis["by_hour"])
            fig = go.Figure()
            fig.add_scatter(x=h["pickup_hour"], y=h["avg_real_min"], name="Réelle", mode="lines",
                            line=dict(color=CYAN, width=2.5))
            fig.add_scatter(x=h["pickup_hour"], y=h["avg_predicted_min"], name="Prédite", mode="lines",
                            line=dict(color=ORANGE, width=2.5, dash="dot"))
            fig.update_layout(xaxis=dict(dtick=1, title="Heure"), yaxis_title="Minutes")
            chart(fig, 300)

        left, right = st.columns(2)
        with left, st.container(border=True):
            title("Erreur du modèle selon la distance")
            dist = pd.DataFrame(kpis["by_distance"])
            order = ["< 1 mile", "1-3 miles", "3-5 miles", "5-10 miles", "> 10 miles"]
            dist["distance_bucket"] = pd.Categorical(dist["distance_bucket"], order, ordered=True)
            dist = dist.sort_values("distance_bucket")
            fig = go.Figure(go.Bar(x=dist["distance_bucket"].astype(str), y=dist["mae_min"], marker_color=CYAN,
                                   text=[f"{v:.2f}" for v in dist["mae_min"]], textposition="outside"))
            fig.update_layout(yaxis_title="MAE (min)")
            chart(fig, 280)
        with right, st.container(border=True):
            title("Répartition des écarts prédiction / réalité")
            err = pd.DataFrame(kpis["error_distribution"])
            order = ["< 1 min", "1-3 min", "3-5 min", "5-10 min", "> 10 min"]
            err["error_bucket"] = pd.Categorical(err["error_bucket"], order, ordered=True)
            err = err.sort_values("error_bucket")
            fig = go.Figure(go.Bar(x=err["error_bucket"].astype(str), y=err["trips"],
                                   marker_color=[GREEN, CYAN, YELLOW, ORANGE, RED]))
            fig.update_layout(yaxis_title="Courses")
            chart(fig, 280)

        with st.container(border=True):
            title("Top 10 des trajets (quartier → quartier)")
            routes = pd.DataFrame(kpis["top_routes"]).rename(columns={
                "pickup_borough": "Départ", "dropoff_borough": "Arrivée", "trips": "Courses",
                "avg_real_min": "Durée moyenne (min)"})
            top = routes["Courses"].max()

            def color_routes(s):
                s = s.map(lambda v: f"background-color: rgba(31,168,201,{0.15 + 0.7 * v / top:.2f}); color:#FFFFFF;",
                          subset=["Courses"])
                return s.format({"Courses": lambda v: fmt(v), "Durée moyenne (min)": "{:.1f}"})
            table(routes, color_routes)

# ============================================================================= 5. DÉRIVE (EVIDENTLY)
with tab5:
    drifts = reports.get("drift", {})
    if not drifts:
        empty("Aucun rapport de dérive pour l'instant.")
    else:
        if drift and drift["status"] != "no_reference":
            c1, c2, c3 = st.columns(3)
            with c1:
                status = "<span class='ko'>Alerte : dérive détectée</span>" if drift["status"] == "alert" \
                    else "<span class='ok'>Pas de dérive significative</span>"
                big_number("Variables en dérive", f"{len(drift['columns_drifted'])}",
                           f" / {drift['columns_monitored']}", sub=status)
            with c2:
                big_number("Part des variables en dérive", f"{100 * drift['drift_share']:.0f}", " %",
                           sub=f"seuil d'alerte : {100 * drift['alert_threshold']:.0f} %")
            with c3:
                delta = drift["avg_duration_current"] - drift["avg_duration_reference"]
                big_number("Durée moyenne des courses", f"{drift['avg_duration_current']:.1f}", " min",
                           sub=f"{delta:+.2f} min vs {drift['reference_month']} (mois de référence)")
        elif drift:
            empty(drift.get("message", "Premier mois : il sert de référence."))

        left, right = st.columns([1, 1.4])
        with left, st.container(border=True):
            title("Dérive par mois", "Par rapport au mois d'entraînement du modèle en production")
            hist = sorted(drifts.items())
            fig = go.Figure(go.Bar(x=[m for m, _ in hist], y=[100 * d.get("drift_share", 0) for _, d in hist],
                                   marker_color=[RED if d["status"] == "alert" else GREY if d["status"] == "no_reference"
                                                 else GREEN for _, d in hist]))
            threshold = next((d["alert_threshold"] for d in drifts.values() if "alert_threshold" in d), None)
            if threshold is not None:
                fig.add_hline(y=100 * threshold, line_dash="dash", line_color=ORANGE,
                              annotation_text="seuil d'alerte", annotation_font_color=ORANGE)
            fig.update_xaxes(type="category")
            fig.update_layout(yaxis_title="% de variables en dérive")
            chart(fig, 340)
        with right, st.container(border=True):
            title("Score de dérive par variable (Evidently)")
            if drift and drift.get("details"):
                det = pd.DataFrame(drift["details"]).sort_values("score")
                fig = go.Figure(go.Bar(x=det["score"], y=det["column"], orientation="h",
                                       marker_color=[RED if d else CYAN for d in det["drifted"]]))
                fig.update_layout(xaxis_title="Score (en rouge : dérive)")
                chart(fig, 340)
            else:
                empty("Pas de détail pour ce mois.")
