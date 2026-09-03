# -*- coding: utf-8 -*-
"""
Dự án theo dõi SLA - Sale
=========================
App Streamlit thay thế cho báo cáo Looker Studio, đọc dữ liệu từ bảng
snapshot BigQuery `track-sale-performance.smart_data.theo_doi_sla`
(bảng này được đổ mỗi ngày bởi Script_process/theo_doi_sla_snapshot.py,
nguồn gốc từ Script_process/uid_current_journey.py).

Giao diện: theme tối (xem .streamlit/config.toml), dieu huong bang tab ngang
(st.tabs), KPI hien thi tran (st.metric) - phong cach dong bo voi du an
GEO PalFish VN (bao cao giam sat AI) da lam truoc do. Tab "Tong quan" la
TRANG MUC LUC - moi nhom chi so o do la 1 "cau hoi" duoc 1 trong 3 tab con lai
tra loi chi tiet (giong cach GEO dan tu tab Tong quan sang cac tab phan tich
sau).

Muc tieu dashboard: theo doi toc do xu ly lead cua Sale, xac dinh SLA dang
nghen o buoc nao va nghen tap trung o Sale nao, tu do tim nguyen nhan de cai
thien ty le tiep can va chuyen doi.

Cấu trúc 4 tab:
  1. Tong quan            - buc tranh chung hom nay the nao? (muc luc)
  2. Diem nghen            - van de nam o khau nao trong hanh trinh?
  3. Hieu suat theo Sale    - van de la cua ai?
  4. Chat luong cuoc goi    - vi sao lai cham/nghen?

Chay app:
    streamlit run app.py
"""
import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# --------------------------------------------------------------------------
# Cau hinh chung
# --------------------------------------------------------------------------
BQ_PROJECT = "track-sale-performance"
BQ_KEY_FILE = r"G:\cosmic-kiln-430904-i8-2ec4bada5c6c.json"
TABLE = "track-sale-performance.smart_data.theo_doi_sla"

# Bang mau dung cho bieu do (van giu ten "blue/peach/..." tu ban thiet ke
# truoc, nhung gio dung lam mau accent tren nen toi thay vi mau nen pastel).
COLORS = {
    "blue": {"accent": "#5B9BD5"},
    "peach": {"accent": "#F0A860"},
    "green": {"accent": "#5CBE8A"},
    "purple": {"accent": "#A98CD6"},
    "red": {"accent": "#E8837A"},
    "yellow": {"accent": "#E0C24A"},
    "gray": {"accent": "#8A8F98"},
}
CHART_FONT_COLOR = "#E5E5E5"

STAGE_TIME_COLS = [
    ("Thoi_gian_L1_phut", "L1 · Chia lead → Cuộc gọi đầu"),
    ("Thoi_gian_L3_1_phut", "L3.1 · Cuộc gọi đầu → Hẹn học thử"),
    ("Thoi_gian_L4_phut", "L4 · Hẹn học thử → Hoàn thành thử"),
    ("Thoi_gian_L5_phut", "L5 · Hoàn thành thử → Tư vấn lộ trình"),
    ("Thoi_gian_L8_phut", "L8 · Tư vấn lộ trình → Mua hàng"),
]

# Dinh nghia CHINH THUC tung moc trong quy trinh xu ly lead cua Sale (Chung
# cung cap 03/09/2026) - dung lam chu thich/so do tren tab "Diem nghen".
LEAD_STAGE_DEFS = [
    ("L1", "New CRM lead", "Lead mới vào hệ thống CRM"),
    ("L2", "Contacted, no trial booked", "Đã liên lạc thành công nhưng chưa đặt lịch học thử"),
    ("L3", "Trial booked", "Đã đặt lịch học thử thành công"),
    ("L4", "Trial completed", "Đã hoàn thành buổi học thử"),
    ("L5", "Learning plan & price shared", "Đã gửi lộ trình học & báo giá sau học thử"),
    ("L6", "Waiting for payment", "Đang chờ khách thanh toán"),
    ("L8", "Purchased & activated", "Đã mua hàng & kích hoạt khóa học"),
]

STAGE_LABEL_MAP = {
    "Hoc thu thanh cong": "Học thử thành công",
    "Mua hang thanh cong": "Mua hàng thành công",
    "Dat lich hoc thu": "Đặt lịch học thử",
}
STAGE_ORDER = [
    "Chưa xác định",
    "Đặt lịch học thử",
    "Học thử thành công",
    "Mua hàng thành công",
]

# Thu tu hien thi Status_of_Lead (trang thai CHINH THUC tu CRM) - khop dung
# thu tu L1 -> L8 cua quy trinh (xem LEAD_STAGE_DEFS), cac bien the KNM/KVHT
# dat ngay sau moc goc cho de doi chieu.
STATUS_OF_LEAD_ORDER = [
    "L1 Chờ gọi",
    "L1 KNM",
    "L2 Đã LL, chưa học thử",
    "L3.1 Có lịch học thử",
    "L3.2 Không vào học thử",
    "L3.3 KVHT, đã gọi",
    "L3.4 KVHT, KNM",
    "L3 Lỗi xếp lớp PalFish",
    "L4 Học thử xong chờ gọi",
    "L5 Đã gọi trả lộ trình",
    "L5 KNM",
    "L6 Chờ thanh toán",
    "L8 Đã nộp đủ học phí",
]

st.set_page_config(
    page_title="Dự án theo dõi SLA - Sale",
    page_icon="📈",
    layout="wide",
)

# --------------------------------------------------------------------------
# BigQuery helpers
# --------------------------------------------------------------------------
@st.cache_resource
def bq_client() -> bigquery.Client:
    # Uu tien doc credentials tu Streamlit Secrets (khi chay tren Streamlit
    # Community Cloud - khong the mang file JSON key len GitHub). Neu khong
    # co file secrets.toml nao ca (chay local tren may Chung, chua cau hinh
    # secrets), st.secrets se NEM LOI ngay khi truy cap (khong chi tra ve
    # rong) - nen phai bat bang try/except, khong dung "in st.secrets" truc
    # tiep.
    try:
        has_secret = "gcp_service_account" in st.secrets
    except Exception:
        has_secret = False

    if has_secret:
        credentials = service_account.Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"])
        )
    else:
        credentials = service_account.Credentials.from_service_account_file(BQ_KEY_FILE)
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


@st.cache_data(ttl=600)
def get_day_range() -> tuple[dt.date, dt.date]:
    client = bq_client()
    q = f"SELECT MIN(day_update) AS mn, MAX(day_update) AS mx FROM `{TABLE}`"
    row = list(client.query(q).result())[0]
    return row["mn"], row["mx"]


@st.cache_data(ttl=600)
def load_sale_list(start: dt.date, end: dt.date) -> list[str]:
    """Danh sach ten sale (Sale_Name_chia_lead) de hien thi trong bo loc
    sidebar. Dung dung cot nay (khong phai Current_Binded_Sale) de khop voi
    cach cac bieu do khac dang gan trach nhiem SLA cho sale."""
    client = bq_client()
    q = f"""
    SELECT DISTINCT Sale_Name_chia_lead AS sale
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      AND Sale_Name_chia_lead IS NOT NULL
    ORDER BY 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
    )
    df = client.query(q, job_config=job_config).result().to_dataframe()
    return df["sale"].tolist()


def _sale_filter(sales: tuple[str, ...] | None):
    """Sinh ra doan SQL + query param de loc theo Sale_Name_chia_lead khi
    nguoi dung chon bo loc sale o sidebar. Tra ve chuoi rong + list rong neu
    khong loc gi (chon tat ca)."""
    if not sales:
        return "", []
    return "AND Sale_Name_chia_lead IN UNNEST(@sales)", [
        bigquery.ArrayQueryParameter("sales", "STRING", list(sales))
    ]


@st.cache_data(ttl=600)
def load_kpi(start: dt.date, end: dt.date, sales: tuple[str, ...] | None = None) -> dict:
    client = bq_client()
    filter_sql, filter_params = _sale_filter(sales)
    q = f"""
    WITH base AS (
      SELECT
        UID, Current_Binded_Sale, Giai_doan_hien_tai, Phan_loai_cuoc_goi_cuoi,
        Thoi_gian_L1_phut, Is_Connect_cuoc_goi_dau_tien,
        TIMESTAMP_DIFF(
          SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', Purchase_Time),
          SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', Thoi_gian_chia_lead),
          MINUTE
        ) AS phut_hoan_thanh_cham_soc
      FROM `{TABLE}`
      WHERE day_update BETWEEN @start AND @end
        {filter_sql}
    )
    SELECT
      COUNT(DISTINCT UID) AS uid_count,
      COUNT(DISTINCT Current_Binded_Sale) AS active_sales,
      APPROX_QUANTILES(Thoi_gian_L1_phut, 2)[OFFSET(1)] AS l1_median,
      AVG(Is_Connect_cuoc_goi_dau_tien) * 100 AS connect_rate,
      AVG(CASE WHEN Giai_doan_hien_tai = 'Mua hang thanh cong'
               THEN 100.0 ELSE 0 END) AS purchase_rate,
      AVG(CASE WHEN Phan_loai_cuoc_goi_cuoi = 'Chua tung goi'
               THEN 100.0 ELSE 0 END) AS never_called_rate,
      -- Thoi gian hoan thanh cham soc trung binh: CHI tinh UID da mua hang
      -- (Purchase_Time - Thoi_gian_chia_lead), dung TRUNG VI de bot bi keo
      -- lech boi cac case am (loi du lieu nguon Metabase, se ra soat lai
      -- sau khi Metabase cap nhat).
      APPROX_QUANTILES(
        IF(Giai_doan_hien_tai = 'Mua hang thanh cong', phut_hoan_thanh_cham_soc, NULL),
        2
      )[OFFSET(1)] AS completion_time_median,
      COUNTIF(Giai_doan_hien_tai = 'Mua hang thanh cong'
              AND phut_hoan_thanh_cham_soc IS NOT NULL) AS completion_n,
      COUNTIF(Giai_doan_hien_tai = 'Mua hang thanh cong'
              AND phut_hoan_thanh_cham_soc < 0) AS completion_n_am
    FROM base
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        + filter_params
    )
    row = list(client.query(q, job_config=job_config).result())[0]
    kpi = dict(row.items())
    kpi["avg_uid_per_sale"] = (
        kpi["uid_count"] / kpi["active_sales"] if kpi["active_sales"] else None
    )
    return kpi


@st.cache_data(ttl=600)
def load_stage_time(
    start: dt.date, end: dt.date, sales: tuple[str, ...] | None = None
) -> pd.DataFrame:
    client = bq_client()
    filter_sql, filter_params = _sale_filter(sales)
    cols_sql = ",\n      ".join(
        f"APPROX_QUANTILES({col}, 2)[OFFSET(1)] AS {col}" for col, _ in STAGE_TIME_COLS
    )
    n_sql = ",\n      ".join(
        f"COUNTIF({col} IS NOT NULL) AS n_{col}" for col, _ in STAGE_TIME_COLS
    )
    q = f"""
    SELECT
      {cols_sql},
      {n_sql}
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      {filter_sql}
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        + filter_params
    )
    row = list(client.query(q, job_config=job_config).result())[0]
    records = [
        {"Chặng": label, "Trung vị (phút)": row[col], "n": row[f"n_{col}"]}
        for col, label in STAGE_TIME_COLS
    ]
    return pd.DataFrame(records)


@st.cache_data(ttl=600)
def load_stage_volume(
    start: dt.date, end: dt.date, sales: tuple[str, ...] | None = None
) -> pd.DataFrame:
    """Phan bo UID theo Status_of_Lead - trang thai CHINH THUC tu CRM (khop
    dung so do L1-L8 chuan cua quy trinh), dung TAM THOI thay cho
    Giai_doan_hien_tai (cot tu tinh trong noi bo, dang co van de "lech vong"
    khi UID da mua roi lai co moc moi - xem trao doi 03/09/2026)."""
    client = bq_client()
    filter_sql, filter_params = _sale_filter(sales)
    q = f"""
    SELECT
      COALESCE(Status_of_Lead, 'Chưa xác định') AS giai_doan,
      COUNT(DISTINCT UID) AS so_uid
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      {filter_sql}
    GROUP BY 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        + filter_params
    )
    df = client.query(q, job_config=job_config).result().to_dataframe()
    df["order"] = df["giai_doan"].apply(
        lambda x: STATUS_OF_LEAD_ORDER.index(x)
        if x in STATUS_OF_LEAD_ORDER else 99
    )
    df = df.sort_values("order")
    return df[["giai_doan", "so_uid"]]


@st.cache_data(ttl=600)
def load_sale_performance(
    start: dt.date, end: dt.date, sales: tuple[str, ...] | None = None
) -> pd.DataFrame:
    """Be so lieu theo tung sale (Sale_Name_chia_lead - sale da nhan lead DUNG LUC
    Thoi_gian_chia_lead, khong phai sale hien tai) - de biet diem nghen o trang
    'Diem nghen' co tap trung o vai sale cu the hay dong deu. Dung cot nay (thay vi
    Current_Binded_Sale) vi no khop dung sale chiu trach nhiem cho L1 (thoi gian
    phan hoi = Thoi_gian_cuoc_goi_dau_tien - Thoi_gian_chia_lead)."""
    client = bq_client()
    filter_sql, filter_params = _sale_filter(sales)
    q = f"""
    SELECT
      COALESCE(Sale_Name_chia_lead, 'Chưa gán sale') AS sale,
      COUNT(DISTINCT UID) AS so_uid,
      AVG(CASE WHEN Giai_doan_hien_tai = 'Mua hang thanh cong'
               THEN 100.0 ELSE 0 END) AS ty_le_mua_hang,
      AVG(CASE WHEN Phan_loai_cuoc_goi_cuoi = 'Chua tung goi'
               THEN 100.0 ELSE 0 END) AS ty_le_chua_tung_goi,
      AVG(Is_Connect_cuoc_goi_dau_tien) * 100 AS ty_le_ket_noi,
      APPROX_QUANTILES(Thoi_gian_L1_phut, 2)[OFFSET(1)] AS l1_median
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      {filter_sql}
    GROUP BY 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        + filter_params
    )
    df = client.query(q, job_config=job_config).result().to_dataframe()
    return df.sort_values("so_uid", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=600)
def load_call_quality(
    start: dt.date, end: dt.date, sales: tuple[str, ...] | None = None
) -> dict:
    """So lieu chat luong cuoc goi: ty le ket noi, so cuoc goi can de ket noi
    duoc, va phan bo so cuoc goi (histogram)."""
    # Tong_so_cuoc_goi_den_khi_nghe_may CHI co gia tri cho UID co cuoc goi DAU
    # TIEN KHONG ket noi (Is_Connect_cuoc_goi_dau_tien = 0) - xem
    # uid_current_journey.py muc "goi_scope". Voi UID ket noi NGAY tu cuoc dau
    # tien, cot nay la NULL (khong phai 1) - phai tu quy dinh = 1 cho nhom do,
    # neu khong se hieu sai (VD tuong nhu khong ai ket noi ngay lan dau).
    so_cuoc_expr = """
      CASE
        WHEN Is_Connect_cuoc_goi_dau_tien = 1 THEN 1
        WHEN Tong_so_cuoc_goi_den_khi_nghe_may IS NOT NULL
          THEN Tong_so_cuoc_goi_den_khi_nghe_may
        ELSE NULL
      END
    """
    client = bq_client()
    filter_sql, filter_params = _sale_filter(sales)
    q = f"""
    SELECT
      COUNT(DISTINCT UID) AS n_da_goi,
      AVG(Is_Connect_cuoc_goi_dau_tien) * 100 AS ty_le_ket_noi,
      AVG(CASE WHEN Is_Connect_cuoc_goi_dau_tien = 0
                    AND Tong_so_cuoc_goi_den_khi_nghe_may IS NULL
               THEN 100.0 ELSE 0 END) AS ty_le_chua_tung_ket_noi,
      APPROX_QUANTILES({so_cuoc_expr}, 2)[OFFSET(1)]
        AS so_cuoc_median_den_ket_noi
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      AND Thoi_gian_cuoc_goi_dau_tien IS NOT NULL
      {filter_sql}
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
        ]
        + filter_params
    )
    kpi = dict(list(client.query(q, job_config=job_config).result())[0].items())

    q2 = f"""
    SELECT
      CASE
        WHEN Is_Connect_cuoc_goi_dau_tien = 0 AND Tong_so_cuoc_goi_den_khi_nghe_may IS NULL
          THEN 'Chưa từng kết nối'
        WHEN {so_cuoc_expr} = 1 THEN '1 cuộc'
        WHEN {so_cuoc_expr} = 2 THEN '2 cuộc'
        WHEN {so_cuoc_expr} = 3 THEN '3 cuộc'
        WHEN {so_cuoc_expr} BETWEEN 4 AND 5 THEN '4-5 cuộc'
        ELSE '6+ cuộc'
      END AS nhom,
      COUNT(DISTINCT UID) AS so_uid
    FROM `{TABLE}`
    WHERE day_update BETWEEN @start AND @end
      AND Thoi_gian_cuoc_goi_dau_tien IS NOT NULL
      {filter_sql}
    GROUP BY 1
    """
    df_hist = client.query(q2, job_config=job_config).result().to_dataframe()
    order = ["1 cuộc", "2 cuộc", "3 cuộc", "4-5 cuộc", "6+ cuộc", "Chưa từng kết nối"]
    df_hist["order"] = df_hist["nhom"].apply(
        lambda x: order.index(x) if x in order else 99
    )
    df_hist = df_hist.sort_values("order")
    kpi["histogram"] = df_hist[["nhom", "so_uid"]]
    return kpi


# --------------------------------------------------------------------------
# UI helpers
# --------------------------------------------------------------------------
def apply_dark_layout(fig: go.Figure, **kwargs):
    """Chuan hoa layout cho tat ca bieu do Plotly tren nen toi: nen trong
    suot (an theo mau nen trang cua Streamlit dark theme), chu sang mau."""
    layout = dict(
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=CHART_FONT_COLOR),
    )
    layout.update(kwargs)
    fig.update_layout(**layout)
    return fig


def render_header():
    st.title("📈 Dự án theo dõi SLA - Sale")
    st.caption(
        "🎯 **Mục tiêu:** theo dõi tốc độ xử lý lead của Sale, xác định SLA "
        "đang nghẽn ở bước nào và nghẽn tập trung ở Sale nào, từ đó tìm "
        "nguyên nhân để cải thiện tỷ lệ tiếp cận và chuyển đổi."
    )


# --------------------------------------------------------------------------
# Sidebar - bo loc
# --------------------------------------------------------------------------
try:
    mn_day, mx_day = get_day_range()
except Exception as e:  # noqa: BLE001
    st.error(f"Không kết nối được BigQuery: {e}")
    st.stop()

st.sidebar.title("⚙️ Thiết lập & bộ lọc")
date_range = st.sidebar.date_input(
    "Ngày snapshot (day_update)",
    value=(mx_day, mx_day),
    min_value=mn_day,
    max_value=mx_day,
)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

sale_options = load_sale_list(start_date, end_date)
selected_sales_list = st.sidebar.multiselect(
    "Lọc theo tên sale (chia lead)",
    options=sale_options,
    help=(
        "Để trống = xem tất cả sale. Lọc theo Sale_Name_chia_lead — sale đã "
        "nhận lead đúng lúc chia lead, dùng chung cho mọi biểu đồ."
    ),
)
selected_sales = tuple(selected_sales_list) if selected_sales_list else None

st.sidebar.caption(f"Dữ liệu snapshot có từ {mn_day} đến {mx_day}.")

# --------------------------------------------------------------------------
# Noi dung chinh - dieu huong bang tab ngang
# --------------------------------------------------------------------------
render_header()

# Tinh KPI 1 lan duy nhat, dung chung cho tab Tong quan VA cac tab con lai
# (de tab con lai dan chieu nguoc ve dung so lieu da nhac o Tong quan, giong
# cach GEO lam - Tong quan la "muc luc", tab sau trich dan lai so cu roi moi
# vao phan tich).
kpi = load_kpi(start_date, end_date, selected_sales)

tab_tong_quan, tab_diem_nghen, tab_sale, tab_call = st.tabs(
    [
        "Tổng quan",
        "Điểm nghẽn",
        "Hiệu suất theo Sale",
        "Chất lượng cuộc gọi",
    ]
)

# --------------------------------------------------------------------------
# Tab: Tong quan - dong vai tro "muc luc": moi nhom chi so duoi day duoc 1
# trong 3 tab con lai dao sau, ghi ro "-> Xem tai tab..." de nguoi xem biet
# bam vao dau de tim hieu tiep.
# --------------------------------------------------------------------------
with tab_tong_quan:
    st.markdown("#### ⏱ Tốc độ xử lý")
    row1 = st.columns(2)
    row1[0].metric("UID đang được chăm sóc", f"{kpi['uid_count']:,}".replace(",", "."))
    row1[1].metric(
        "L1 · Trung vị (phút)",
        f"{kpi['l1_median']:,.0f}" if kpi["l1_median"] is not None else "—",
    )
    st.caption("→ Xem chặng nào đang chậm nhất tại tab **Điểm nghẽn**.")

    st.markdown("#### 🎯 Hiệu suất chuyển đổi")
    row2 = st.columns(2)
    row2[0].metric(
        "Tỷ lệ mua hàng thành công",
        f"{kpi['purchase_rate']:.0f}%" if kpi["purchase_rate"] is not None else "—",
    )
    row2[1].metric("Số sale đang hoạt động", f"{kpi['active_sales']:,}")
    st.caption("→ Xem hiệu suất này chia theo từng sale tại tab **Hiệu suất theo Sale**.")

    st.markdown("#### 📞 Chất lượng tiếp cận")
    row3 = st.columns(2)
    row3[0].metric(
        "Tỷ lệ kết nối cuộc gọi đầu",
        f"{kpi['connect_rate']:.0f}%" if kpi["connect_rate"] is not None else "—",
    )
    row3[1].metric(
        "Tỷ lệ chưa từng gọi",
        f"{kpi['never_called_rate']:.0f}%"
        if kpi["never_called_rate"] is not None
        else "—",
    )
    st.caption("→ Xem vì sao khó kết nối tại tab **Chất lượng cuộc gọi**.")

    st.divider()
    st.caption(
        "💡 Mẹo: đổi khoảng ngày ở thanh bên trái để xem lại lịch sử snapshot "
        "các ngày trước."
    )

# --------------------------------------------------------------------------
# Tab: Diem nghen
# --------------------------------------------------------------------------
with tab_diem_nghen:
    with st.expander("📋 Quy trình xử lý lead của Sale (định nghĩa từng mốc)"):
        for code, en, vi in LEAD_STAGE_DEFS:
            st.markdown(f"**{code}** — {en}: {vi}")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**⏱ Thời gian trung vị mỗi chặng (phút)**")
        df_time = load_stage_time(start_date, end_date, selected_sales)
        # Case nghi ngo: n qua nho (<10) hoac gia tri am (khong the co thoi
        # gian am) - thuong la do du lieu nguon (Metabase) chua day du /
        # sai lech, KHONG loc bo, chi to mau canh bao de de nhan biet.
        is_suspect = (df_time["n"] < 10) | (df_time["Trung vị (phút)"] < 0)
        bar_colors = [
            COLORS["gray"]["accent"] if s else COLORS["peach"]["accent"]
            for s in is_suspect
        ]
        labels = [
            f"{v:,.0f} (n={n})" if pd.notna(v) else f"— (n={n})"
            for v, n in zip(df_time["Trung vị (phút)"], df_time["n"])
        ]
        fig1 = go.Figure(
            go.Bar(
                x=df_time["Trung vị (phút)"],
                y=df_time["Chặng"],
                orientation="h",
                marker=dict(color=bar_colors),
                text=labels,
                textposition="outside",
            )
        )
        apply_dark_layout(fig1, height=360, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig1, use_container_width=True)
        if is_suspect.any():
            st.caption(
                "⚪ Chặng tô xám: cỡ mẫu (n) quá nhỏ hoặc ra thời gian âm — "
                "nghi do dữ liệu nguồn (Metabase) chưa đầy đủ, cần kiểm tra "
                "lại khi dữ liệu Metabase được cập nhật."
            )

    with col_right:
        st.markdown("**👥 Số UID theo trạng thái lead (Status_of_Lead)**")
        df_vol = load_stage_volume(start_date, end_date, selected_sales)
        accent_cycle = [
            COLORS["red"]["accent"], COLORS["peach"]["accent"],
            COLORS["yellow"]["accent"], COLORS["green"]["accent"],
            COLORS["blue"]["accent"], COLORS["purple"]["accent"],
            COLORS["gray"]["accent"],
        ]
        palette = [accent_cycle[i % len(accent_cycle)] for i in range(len(df_vol))]
        fig2 = go.Figure(
            go.Bar(
                x=df_vol["so_uid"],
                y=df_vol["giai_doan"],
                orientation="h",
                marker=dict(color=palette),
                text=df_vol["so_uid"].map(lambda v: f"{v:,}"),
                textposition="outside",
            )
        )
        apply_dark_layout(
            fig2, height=max(360, 30 * len(df_vol)),
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.caption(
        "📋 Số UID theo trạng thái lấy từ `Status_of_Lead` (trạng thái chính "
        "thức từ CRM) — dùng tạm thay cho `Giai_doan_hien_tai` (cột tự tính "
        "nội bộ, đang có case bị lệch vòng khi UID đã mua rồi lại có mốc mới)."
    )
    st.info(
        "Đọc biểu đồ: quy trình sale KHÔNG bắt buộc đi qua đủ mọi giai đoạn "
        "(có thể bỏ qua bước) nên biểu đồ bên phải **không phải là phễu** — chỉ "
        "cho biết UID đang **đứng ở đâu ngay bây giờ**, không dùng để tính tỷ lệ "
        "rớt giữa 2 giai đoạn. Kết hợp với biểu đồ bên trái để xem: giai đoạn "
        "nào vừa xử lý chậm (bên trái) vừa có nhiều UID đang đứng ở đó (bên "
        "phải) → khâu đó đang ùn ứ, cần ưu tiên xử lý."
    )

# --------------------------------------------------------------------------
# Tab: Hieu suat theo Sale
# --------------------------------------------------------------------------
with tab_sale:
    df_sale = load_sale_performance(start_date, end_date, selected_sales)

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("**👤 Số UID đang chăm theo sale (top 15)**")
        top15 = df_sale.head(15).sort_values("so_uid")
        fig3 = go.Figure(
            go.Bar(
                x=top15["so_uid"],
                y=top15["sale"],
                orientation="h",
                marker=dict(color=COLORS["blue"]["accent"]),
                text=top15["so_uid"].map(lambda v: f"{v:,}"),
                textposition="outside",
            )
        )
        apply_dark_layout(fig3, height=420)
        st.plotly_chart(fig3, use_container_width=True)

    # Nguong "khoi luong nhieu" = top 25% sale co nhieu UID nhat (P75), nguong
    # "ty le thap" = trung binh chung - tao thanh 1 vung "dang lo" ro rang thay
    # vi bat nguoi xem tu doan qua 149 diem chi chit.
    workload_threshold = df_sale["so_uid"].quantile(0.75)
    avg_purchase = df_sale["ty_le_mua_hang"].mean()
    df_sale["dang_lo"] = (df_sale["so_uid"] >= workload_threshold) & (
        df_sale["ty_le_mua_hang"] < avg_purchase
    )
    canh_bao = df_sale[df_sale["dang_lo"]].sort_values("so_uid", ascending=False)

    with col_right:
        st.markdown("**🎯 Khối lượng vs. Tỷ lệ mua hàng — sale nào cần chú ý**")
        fig4 = go.Figure()
        # Vung to mau (glow do nhat) : "nhieu viec + ty le thap" - vung dang lo
        # ngay tren bieu do, thay vi bat nguoi xem tu doan qua tram diem.
        fig4.add_shape(
            type="rect",
            x0=workload_threshold, x1=df_sale["so_uid"].max() * 1.05,
            y0=0, y1=avg_purchase,
            fillcolor=COLORS["red"]["accent"], opacity=0.16, line_width=0,
            layer="below",
        )
        binh_thuong = df_sale[~df_sale["dang_lo"]]
        fig4.add_trace(go.Scatter(
            x=binh_thuong["so_uid"], y=binh_thuong["ty_le_mua_hang"],
            mode="markers",
            marker=dict(size=9, color=COLORS["purple"]["accent"], opacity=0.6,
                        line=dict(width=1, color="#3D3250")),
            text=binh_thuong["sale"],
            hovertemplate="%{text}<br>Số UID: %{x}<br>Tỷ lệ mua hàng: %{y:.0f}%<extra></extra>",
            name="Bình thường",
        ))
        fig4.add_trace(go.Scatter(
            x=canh_bao["so_uid"], y=canh_bao["ty_le_mua_hang"],
            mode="markers+text",
            marker=dict(size=13, color=COLORS["red"]["accent"],
                        line=dict(width=1.5, color="#7A2E22")),
            text=canh_bao["sale"],
            textposition="top center",
            textfont=dict(size=10, color=COLORS["red"]["accent"]),
            hovertemplate="%{text}<br>Số UID: %{x}<br>Tỷ lệ mua hàng: %{y:.0f}%<extra></extra>",
            name="Cần chú ý",
        ))
        fig4.add_hline(
            y=avg_purchase, line_dash="dash", line_color=COLORS["red"]["accent"],
            annotation_text=f"TB chung: {avg_purchase:.0f}%",
            annotation_font_color=CHART_FONT_COLOR,
        )
        apply_dark_layout(
            fig4, height=420,
            xaxis_title="Số UID đang chăm (khối lượng việc)",
            yaxis_title="Tỷ lệ mua hàng (%)",
            showlegend=False,
        )
        st.plotly_chart(fig4, use_container_width=True)

    if len(canh_bao) > 0:
        ten_list = ", ".join(canh_bao["sale"].head(8).tolist())
        st.warning(
            f"🔴 Vùng đỏ = sale vừa chăm **≥{workload_threshold:.0f} UID** "
            f"(top 25% khối lượng) vừa tỷ lệ mua hàng **dưới mức TB chung "
            f"({avg_purchase:.0f}%)** — {len(canh_bao)} sale rơi vào nhóm này, "
            f"đáng ưu tiên hỗ trợ/kiểm tra trước: **{ten_list}**"
            + (f" và {len(canh_bao) - 8} sale khác." if len(canh_bao) > 8 else ".")
        )
    else:
        st.success("Không có sale nào vừa khối lượng cao vừa tỷ lệ mua hàng thấp hơn TB chung.")

    st.markdown("**📋 Bảng chi tiết theo sale**")
    df_display = df_sale.rename(
        columns={
            "sale": "Sale",
            "so_uid": "Số UID đang chăm",
            "ty_le_mua_hang": "Tỷ lệ mua hàng (%)",
            "ty_le_chua_tung_goi": "Tỷ lệ chưa từng gọi (%)",
            "ty_le_ket_noi": "Tỷ lệ kết nối cuộc gọi đầu (%)",
            "l1_median": "L1 · Trung vị (phút)",
        }
    )
    for c in [
        "Tỷ lệ mua hàng (%)", "Tỷ lệ chưa từng gọi (%)",
        "Tỷ lệ kết nối cuộc gọi đầu (%)",
    ]:
        df_display[c] = df_display[c].round(0)
    st.dataframe(df_display, use_container_width=True, hide_index=True)
    st.caption(
        "💡 Bấm vào tiêu đề cột để sắp xếp lại bảng (VD: sắp theo tỷ lệ mua "
        "hàng thấp nhất lên đầu để xem sale nào cần chú ý)."
    )

# --------------------------------------------------------------------------
# Tab: Chat luong cuoc goi
# --------------------------------------------------------------------------
with tab_call:
    cq = load_call_quality(start_date, end_date, selected_sales)

    row1 = st.columns(3)
    row1[0].metric(
        "Tỷ lệ kết nối cuộc gọi đầu",
        f"{cq['ty_le_ket_noi']:.0f}%" if cq["ty_le_ket_noi"] is not None else "—",
    )
    row1[1].metric(
        "Chưa từng kết nối dù gọi nhiều lần",
        f"{cq['ty_le_chua_tung_ket_noi']:.0f}%"
        if cq["ty_le_chua_tung_ket_noi"] is not None else "—",
    )
    row1[2].metric(
        "Số cuộc gọi trung vị để kết nối",
        f"{cq['so_cuoc_median_den_ket_noi']:.0f}"
        if cq["so_cuoc_median_den_ket_noi"] is not None else "—",
    )

    st.markdown("**📞 Phân bố số cuộc gọi cần để kết nối được với khách**")
    df_hist = cq["histogram"]
    colors_hist = [
        COLORS["red"]["accent"] if nhom == "Chưa từng kết nối"
        else COLORS["blue"]["accent"]
        for nhom in df_hist["nhom"]
    ]
    fig5 = go.Figure(
        go.Bar(
            x=df_hist["nhom"], y=df_hist["so_uid"],
            marker=dict(color=colors_hist),
            text=df_hist["so_uid"].map(lambda v: f"{v:,}"),
            textposition="outside",
        )
    )
    apply_dark_layout(
        fig5, height=380,
        xaxis_title="Số cuộc gọi cần để kết nối",
        yaxis_title="Số UID",
    )
    st.plotly_chart(fig5, use_container_width=True)
    st.info(
        "Đọc biểu đồ: cột càng lệch về bên phải (cần nhiều cuộc gọi mới bắt "
        "máy) hoặc cột **Chưa từng kết nối** (đỏ) càng cao → càng khó tiếp "
        "cận khách, có thể do sai số điện thoại, khách không nghe máy lạ, "
        "hoặc thời điểm gọi chưa phù hợp."
    )
