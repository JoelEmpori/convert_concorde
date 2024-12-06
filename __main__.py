import subprocess
from pathlib import Path

def run_app():
    import sys
    from pathlib import Path
    dashboard_path = Path(__file__).parent / "dashboard.py"
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard_path)])

if __name__ == '__main__':
    run_app()
