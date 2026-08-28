FROM docker.io/library/clojure:tools-deps-trixie-slim

WORKDIR /app

COPY deps.edn ./
RUN clojure -P

COPY src ./src

CMD ["clojure", "-M", "-m", "httparena.capra.core"]
