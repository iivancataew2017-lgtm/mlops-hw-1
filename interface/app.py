import hashlib
import json
import os
import uuid

import pandas as pd
import streamlit as st
from kafka import KafkaProducer


KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions"),
}


def load_file(uploaded_file):
    df = pd.read_csv(uploaded_file, dtype={"transaction_id": "string"})
    if df.empty:
        raise ValueError("В CSV нет транзакций")
    if "transaction_id" not in df:
        df["transaction_id"] = None
    # ID назначается при загрузке, чтобы повторная отправка не создавала дубли в БД.
    df["transaction_id"] = [
        str(uuid.uuid4()) if pd.isna(value) or not str(value).strip() else str(value).strip()
        for value in df["transaction_id"]
    ]
    if df["transaction_id"].duplicated().any():
        raise ValueError("В CSV есть повторяющиеся transaction_id")
    return df


def send_to_kafka(df, topic, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=10000,
        request_timeout_ms=10000,
    )
    progress_bar = st.progress(0)
    try:
        records = df.astype(object).where(pd.notna(df), None).to_dict(orient="records")
        for index, record in enumerate(records, start=1):
            transaction_id = record.pop("transaction_id")
            producer.send(
                topic,
                key=transaction_id,
                value={"transaction_id": transaction_id, "data": record},
            ).get(timeout=30)
            progress_bar.progress(index / len(records))
    finally:
        producer.close(timeout=10)


def main():
    st.title("Отправка транзакций")
    uploaded_files = st.session_state.setdefault("uploaded_files", {})
    uploaded_file = st.file_uploader("Загрузите CSV файл с транзакциями", type=["csv"])

    if uploaded_file is not None:
        file_key = hashlib.sha256(uploaded_file.getvalue()).hexdigest()
        if file_key not in uploaded_files:
            try:
                df = load_file(uploaded_file)
            except (ValueError, UnicodeError) as error:
                st.error(f"Не удалось прочитать CSV: {error}")
            else:
                uploaded_files[file_key] = {"name": uploaded_file.name, "status": "Загружен", "df": df}

    if uploaded_files:
        st.subheader("Загруженные файлы")
    for file_key, file_data in uploaded_files.items():
        details, action = st.columns([4, 2])
        with details:
            st.write(file_data["name"])
            st.caption(f"{file_data['status']} · {len(file_data['df'])} транзакций")
        with action:
            if st.button("Отправить", key=f"send_{file_key}"):
                try:
                    with st.spinner("Отправка..."):
                        send_to_kafka(file_data["df"], KAFKA_CONFIG["topic"], KAFKA_CONFIG["bootstrap_servers"])
                except Exception as error:
                    st.error(f"Ошибка отправки: {error}. Можно повторить отправку с теми же ID.")
                else:
                    file_data["status"] = "Отправлен"
                    st.rerun()


if __name__ == "__main__":
    main()
