.PHONY: build clean publish publish-test

build:
	py -m build

publish:
	$(build)
	py -m twine upload dist/*

publish-test:
	$(build)
	py -m twine upload --repository testpypi dist/*

clean:
	rm dist/*
	rm dist/
