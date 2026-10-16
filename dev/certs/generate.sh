#!/usr/bin/env bash
# Тестовые сертификаты для mTLS-листенера dev-стенда и тестов. ТОЛЬКО ДЛЯ РАЗРАБОТКИ.
# Требует openssl (OpenSSL или LibreSSL) и docker (keytool берётся из образа Kafka).
set -euo pipefail
cd "$(dirname "$0")"
OUT=out
P=storepass
DAYS=3650
rm -rf "$OUT" && mkdir -p "$OUT"

openssl req -x509 -newkey rsa:2048 -nodes -days $DAYS -subj "/CN=ke-dev-ca" \
  -keyout $OUT/ca.key -out $OUT/ca.pem

# Брокер: имя localhost / kafka / 127.0.0.1
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout $OUT/broker.key -out $OUT/broker.csr
printf "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1\n" > $OUT/broker.ext
openssl x509 -req -in $OUT/broker.csr -CA $OUT/ca.pem -CAkey $OUT/ca.key -CAcreateserial \
  -days $DAYS -extfile $OUT/broker.ext -out $OUT/broker.pem
openssl pkcs12 -export -name broker -in $OUT/broker.pem -inkey $OUT/broker.key \
  -certfile $OUT/ca.pem -passout pass:$P -out $OUT/broker.p12
printf "%s" "$P" > $OUT/broker_creds

# Клиент
openssl req -newkey rsa:2048 -nodes -subj "/CN=ke-client" -keyout $OUT/client.key -out $OUT/client.csr
printf "extendedKeyUsage=clientAuth\n" > $OUT/client.ext
openssl x509 -req -in $OUT/client.csr -CA $OUT/ca.pem -CAkey $OUT/ca.key -CAcreateserial \
  -days $DAYS -extfile $OUT/client.ext -out $OUT/client.pem
openssl pkcs8 -topk8 -v2 aes-256-cbc -in $OUT/client.key -passout pass:keypass -out $OUT/client-enc.key
openssl pkcs12 -export -name client -in $OUT/client.pem -inkey $OUT/client.key \
  -certfile $OUT/ca.pem -passout pass:$P -out $OUT/client.p12
openssl pkcs12 -export -name client -in $OUT/client.pem -inkey $OUT/client.key \
  -certfile $OUT/ca.pem -passout pass: -out $OUT/client-nopass.p12
# Старые алгоритмы (RC2/3DES) — для проверки legacy-провайдера OpenSSL 3 (нужен OpenSSL 3.x)
openssl pkcs12 -export -legacy -name client -in $OUT/client.pem -inkey $OUT/client.key \
  -certfile $OUT/ca.pem -passout pass:$P -out $OUT/client-legacy.p12

# keytool (Java) — из образа Kafka
KT=(docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/$OUT:/out" -w /out --entrypoint keytool confluentinc/cp-kafka:7.6.1)
"${KT[@]}" -importcert -noprompt -alias ca -file ca.pem -keystore truststore.p12 -storetype PKCS12 -storepass $P
"${KT[@]}" -importcert -noprompt -alias ca -file ca.pem -keystore truststore.jks -storetype JKS -storepass $P
"${KT[@]}" -importkeystore -noprompt -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass $P \
  -srcalias client -destalias client -destkeystore client.jks -deststoretype JKS -deststorepass $P -destkeypass $P
"${KT[@]}" -importkeystore -noprompt -srckeystore client.p12 -srcstoretype PKCS12 -srcstorepass $P \
  -srcalias client -destalias client -destkeystore client-keypass.jks -deststoretype JKS -deststorepass $P -destkeypass keypass

rm -f $OUT/*.csr $OUT/*.ext $OUT/*.srl
echo "Готово: $(ls $OUT | tr '\n' ' ')"
