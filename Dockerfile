FROM alpine:3.5

# Install librdkafka
ENV LIBRDKAFKA_NAME="librdkafka" \
    LIBRDKAFKA_VERSION="0.9.3"
RUN BUILD_DIR="$(mktemp -d)" && \
\
    apk add --no-cache --virtual .fetch-deps \
      ca-certificates \
      libressl \
      tar && \
    wget -O "$BUILD_DIR/$LIBRDKAFKA_NAME.tar.gz" "https://github.com/edenhill/librdkafka/archive/v$LIBRDKAFKA_VERSION.tar.gz" && \
    mkdir -p "$BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VERSION" && \
    tar \
      --extract \
      --file "$BUILD_DIR/$LIBRDKAFKA_NAME.tar.gz" \
      --directory "$BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VERSION" \
      --strip-components 1 && \
\
    apk add --no-cache --virtual .build-deps \
      bash \
      build-base \
      libressl-dev \
      python \
      zlib-dev && \
    cd "$BUILD_DIR/$LIBRDKAFKA_NAME-$LIBRDKAFKA_VERSION" && \
    ./configure \
      --prefix=/usr && \
    make -j "$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
\
    runDeps="$( \
      scanelf --needed --nobanner --recursive /usr/local \
        | awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
        | sort -u \
        | xargs -r apk info --installed \
        | sort -u \
      )" && \
    apk add --no-cache --virtual .librdkafka-rundeps \
      $runDeps && \
\
    apk del .fetch-deps .build-deps && \
    rm -rf $BUILD_DIR

# Install avro-c
ENV AVRO_C_NAME="avro-c" \
    AVRO_C_VERSION="1.8.1"
RUN BUILD_DIR="$(mktemp -d)" && \
\
    apk add --no-cache --virtual .fetch-deps \
      tar && \
    wget -O "$BUILD_DIR/$AVRO_C_NAME.tar.gz" "http://archive.apache.org/dist/avro/avro-${AVRO_C_VERSION}/c/$AVRO_C_NAME-$AVRO_C_VERSION.tar.gz" && \
    mkdir -p "$BUILD_DIR/$AVRO_C_NAME-$AVRO_C_VERSION" && \
    tar \
      --extract \
      --file "$BUILD_DIR/$AVRO_C_NAME.tar.gz" \
      --directory "$BUILD_DIR/$AVRO_C_NAME-$AVRO_C_VERSION" \
      --strip-components 1 && \
\
    apk add --no-cache --virtual .build-deps \
      bash \
      build-base \
      cmake \
      jansson-dev \
      snappy-dev \
      zlib-dev && \
    cd "$BUILD_DIR/$AVRO_C_NAME-$AVRO_C_VERSION" && \
    cmake . -DCMAKE_INSTALL_PREFIX=/usr && \
    make -j "$(getconf _NPROCESSORS_ONLN)" && \
    make install && \
\
    runDeps="$( \
      scanelf --needed --nobanner --recursive /usr/local \
        | awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
        | sort -u \
        | xargs -r apk info --installed \
        | sort -u \
      )" && \
    apk add --no-cache --virtual .avro-c-rundeps \
      $runDeps && \
\
    apk del .fetch-deps .build-deps && \
    rm -rf $BUILD_DIR

LABEL maintainer="King Chung Huang <kchuang@ucalgary.ca>" \
	  org.label-schema.schema-version="1.0" \
	  org.label-schema.name="Bottled Water Client" \
	  org.label-schema.url="http://blog.confluent.io/2015/04/23/bottled-water-real-time-integration-of-postgresql-and-kafka/" \
    org.label-schema.vcs-url="https://github.com/ucalgary/bottledwater-pg"
