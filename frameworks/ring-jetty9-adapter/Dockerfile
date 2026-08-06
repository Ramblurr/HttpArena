FROM docker.io/library/clojure:tools-deps-trixie-slim

WORKDIR /app

ENV JAVA_TOOL_OPTIONS="--enable-native-access=ALL-UNNAMED"

COPY deps.edn ./
RUN clojure -X:deps prep

COPY src ./src

CMD ["clojure", "-M", "-m", "httparena.ring-jetty9-adapter.core"]
