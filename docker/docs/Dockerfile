FROM squidfunk/mkdocs-material:9.6

RUN apk update
RUN apk add gcc musl-dev linux-headers
RUN pip install mkdocs-macros-plugin \
    mkdocs-git-revision-date-localized-plugin \
    mkdocs-jupyter \
    mike
