.PHONY: test smoke smoke-fast lint lint-core dev restart

lint-core:
	@bad=$$(find app/core -type f \( -name "*.bak*" -o -name "* v*.py" -o -name "*copy*.py" -o -name "*backup*" -o -name "*.xlsx" -o -name "*.docx" -o -name "*.pdf" \) 2>/dev/null); \
	if [ -n "$$bad" ]; then echo "ERROR: archivos prohibidos en app/core/:"; echo "$$bad"; exit 1; fi
	@dirs=$$(find app/core -type d -name "backup*" 2>/dev/null); \
	if [ -n "$$dirs" ]; then echo "ERROR: directorios prohibidos en app/core/:"; echo "$$dirs"; exit 1; fi
	@echo "app/core/ limpio"

test: lint-core
	source .venv/bin/activate && python -m pytest tests/ -q

smoke:
	python scripts/smoke_test.py

smoke-fast:
	python scripts/smoke_test.py --fast

lint:
	ruff check app/ tests/ scripts/ --select E,W,F --ignore E501

dev:
	./dev.sh

restart:
	pkill -f "uvicorn app.main:app" || true
	pkill -f "streamlit run streamlit_app.py" || true
	./dev.sh
