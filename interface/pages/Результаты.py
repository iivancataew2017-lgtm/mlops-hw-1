import os

import pandas as pd
import psycopg
import streamlit as st


POSTGRES_DSN = os.environ["POSTGRES_DSN"]


def load_results():
    with psycopg.connect(POSTGRES_DSN) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT transaction_id, score, fraud_flag::int AS fraud_flag, created_at
            FROM transaction_scores
            WHERE fraud_flag = TRUE
            ORDER BY created_at DESC, transaction_id DESC
            LIMIT 10
            """
        )
        fraud = pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])
        cursor.execute(
            """
            SELECT score
            FROM transaction_scores
            ORDER BY created_at DESC, transaction_id DESC
            LIMIT 100
            """
        )
        scores = pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])
    return fraud, scores


st.title("Результаты скоринга")

if st.button("Посмотреть результаты", type="primary"):
    try:
        fraud_rows, score_rows = load_results()
        st.subheader("10 последних транзакций с fraud_flag = 1")
        if fraud_rows.empty:
            st.info("Fraud-транзакций пока нет.")
        else:
            st.dataframe(fraud_rows, use_container_width=True, hide_index=True)

        st.subheader("Распределение скоров последних 100 транзакций")
        if score_rows.empty:
            st.info("В базе пока нет результатов скоринга.")
        else:
            bins = pd.cut(score_rows["score"], bins=[index / 10 for index in range(11)], include_lowest=True)
            histogram = bins.value_counts(sort=False).rename_axis("Скор").reset_index(name="Транзакции")
            histogram["Скор"] = histogram["Скор"].astype(str)
            st.bar_chart(histogram.set_index("Скор"))
            st.caption(f"Транзакций в выборке: {len(score_rows)}")
    except Exception as error:
        st.error(f"Не удалось получить результаты из PostgreSQL: {error}")
