git_branch := $(shell basename $(shell git rev-parse --abbrev-ref HEAD))
blubber_lives_in := /srv/service
doc_builddir := doc/build
apidocs_json := ${doc_builddir}/apidocs-${git_branch}.json
apidocs_html := ${doc_builddir}/apidocs-${git_branch}.html
htmldocs_image := sockpuppet-htmldocs
apidocs_image := sockpuppet-apidocs

.PHONY: venv docker dockerfile apidocs htmldocs test

venv: requirements.txt
	test -d venv || virtualenv --python=$(shell which python3) venv
	. venv/bin/activate; pip install -Ur requirements.txt; python setup.py install;

dockerfile: .pipeline/blubber.yaml
	blubber .pipeline/blubber.yaml development > Dockerfile

docker: dockerfile
	docker build -t sockpuppet  .
	docker run -p 5000:5000 sockpuppet

test:	venv
	. venv/bin/activate; pytest --cov similar_users

doc_builddir:
	rm -fr ${doc_builddir}
	mkdir -p ${doc_builddir}

apidocs: doc_builddir
	blubber .pipeline/blubber.yaml apidocs | docker build  -t ${apidocs_image} -f - .
	$(eval container_id = $(shell docker run -d ${apidocs_image} bash -c "PYTHONPATH=/opt/lib/python/site-packages:${blubber_lives_in} python3 doc/specgen.py -o ${apidocs_json}"))
	docker wait ${container_id}
	docker cp ${container_id}:${blubber_lives_in}/${apidocs_json} doc/build/

htmldocs: apidocs
	blubber .pipeline/blubber.yaml htmldocs | docker build -t ${htmldocs_image} -f - .
	$(eval container_id = $(shell docker run -d ${htmldocs_image} npx redoc-cli bundle ${apidocs_json} --output ${apidocs_html}))
	docker wait ${container_id}
	docker cp ${container_id}:${blubber_lives_in}/${apidocs_html} doc/build/
