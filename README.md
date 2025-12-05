# Realtime Video/Social Pipeline (Local Delta)

간단한 로컬 파이프라인으로 랜딩→브론즈→실버(스트리밍)→골드→예측→대시보드까지 이어지는 샘플입니다. Kafka/MinIO 없이 파일 기반이며 Delta Lake를 사용합니다.

## 구성
- `jobs/00_fetch_to_landing.py` : 모의 데이터 fetch → `data/landing` NDJSON
- `jobs/10_bronze_batch.py` : 브론즈 배치 적재 + Bloom 프리체크
- `jobs/20_silver_stream.py` : 파일 스트리밍 + Sliding Window(1h/5m) + Watermark + uniq_authors_est
- `jobs/30_gold_features.py` : HLL(approx_count_distinct) + CDF 컷 라벨링
- `jobs/40_train_pareto.py` : 다모델(AUC/지연/피처수) 파레토 전선
- `jobs/50_predict_stream.py` : 간단한 예측 append
- `app/app.py` : Streamlit 대시보드(CDF/PDF, Bloom 존재 여부, Top-K 상승률)
- `conf/app.yaml`, `.env` : 경로/파라미터 설정, `conf/logging.yaml` 로깅

## 빠른 시작 (WSL 기준)
1) 의존성 설치
```bash
python3 -m pip install -r requirements.txt
```
2) JDK 준비: `./.jdk/temurin11` (이미 다운로드되어 있음)  
3) 환경 변수(공통)  
`source env.sh` 로 한 번에 설정 (WSL/bash 기준)

## 전체 실행 순서 (수동)
```bash
# 0. 샘플 데이터 생성
bash scripts/make_sample_data.sh

# 1. 브론즈 배치
python3 jobs/10_bronze_batch.py

# 2. 실버 스트림 (별도 터미널에서 켜두기, 종료는 Ctrl+C)
PYSPARK_SUBMIT_ARGS="--conf spark.databricks.delta.schema.autoMerge.enabled=true --conf spark.jars.ivy=$(pwd)/.ivy2 --packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell" \
python3 jobs/20_silver_stream.py

# 3. 골드/파레토/예측
python3 jobs/30_gold_features.py
python3 jobs/40_train_pareto.py
python3 jobs/50_predict_stream.py
```

## 자동 루프 예시 (WSL)
- 5초마다 랜딩 데이터 생성:
```bash
while true; do bash scripts/make_sample_data.sh; sleep 5; done
```
- 30초마다 배치:
```bash
while true; do
  python3 jobs/10_bronze_batch.py
  python3 jobs/30_gold_features.py
  python3 jobs/40_train_pareto.py
  python3 jobs/50_predict_stream.py
  sleep 30
done
```
- 실버 스트림은 별도 터미널에서 상시 실행.

## Streamlit 대시보드
```bash
PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.0 pyspark-shell" \
STREAMLIT_SERVER_HEADLESS=true streamlit run app/app.py --server.headless true --browser.gatherUsageStats false
```
브라우저에서 localhost 접속.

## 유의사항
- Delta 패키지가 필수입니다 (`io.delta:delta-spark_2.12:3.2.0`).
- WSL에서 파이프라인을 돌렸다면 Streamlit도 WSL에서 동일한 경로/옵션으로 실행하세요.
- `spark.sql.adaptive.enabled`는 스트리밍에서 비활성화됩니다(경고 무시 가능).
