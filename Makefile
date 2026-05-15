.PHONY: test smoke smoke-fast lint

test:
	pytest tests/ -q --tb=short

smoke:
	python scripts/smoke_test.py

smoke-fast:
	python scripts/smoke_test.py --fast

lint:
	ruff check app/ tests/ scripts/ --select E,W,F --ignore E501
dev:
	./dev.sh

test:
	source .venv/bin/activate && python -m pytest tests/ -q

restart:
	pkill -f "uvicorn app.main:app" || true
	pkill -f "streamlit run streamlit_app.py" || true
	./dev.sh
