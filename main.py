import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
import logging

def esta_em_venv():
    return (
        hasattr(sys, 'real_prefix') or
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or
        os.environ.get("VIRTUAL_ENV") is not None
    )

def garantir_venv():
    base_dir = Path(__file__).resolve().parent
    venv_dir = base_dir / ".venv"

    if not venv_dir.exists():
        print("[⚙️ ] Ambiente virtual '.venv' não encontrado. Criando...")
        subprocess.check_call([sys.executable, "-m", "venv", str(venv_dir)])
        print("[✓] Ambiente virtual criado com sucesso.")

    if not esta_em_venv():
        print("[🔄] Reiniciando o script dentro do ambiente virtual...")
        python_venv = venv_dir / "bin" / "python"
        os.execv(str(python_venv), [str(python_venv)] + sys.argv)

garantir_venv()

# ---------------------------------------------------------------------------------------------------------------------

PASTAS = ['doc', 'notebooks', 'scripts', 'pdf', '.info', '.log', '.functions', 'markdown']

# Logger global (provisório, será configurado depois)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------------------------------------------------
def criar_funcoes_padrao(functions_dir: Path):
    logger.info(f"[*] Inicializando arquivos padrão em {functions_dir}")

    # __init__.py
    (functions_dir / "__init__.py").write_text("")

    # log.py
    log_code = '''\
import logging
import sys
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def configurar_logger(base_dir: Path):
    log_dir = base_dir / ".log"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_filename = datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".log"
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger.info(f"Arquivo de log criado: {log_path}")
    return logger
'''
    (functions_dir / "log.py").write_text(log_code)
    logger.info("[+] Criado: log.py")

    # estrutura.py
    estrutura_code = '''\
def criar_pastas(base_dir, pastas, logger):
    for pasta in pastas:
        dir_path = base_dir / pasta
        if not dir_path.exists():
            dir_path.mkdir(parents=True)
            logger.info(f"[+] Criada pasta: {dir_path}")
        else:
            logger.info(f"[=] Pasta já existe: {dir_path}")
        if pasta == ".functions":
            from pathlib import Path
            from main import criar_funcoes_padrao
            criar_funcoes_padrao(dir_path)
'''
    (functions_dir / "estrutura.py").write_text(estrutura_code)
    logger.info("[+] Criado: estrutura.py")

    # conversao.py
    conversao_code = '''\
import subprocess
import sys

def converte_to_md(arquivo_path, base_dir, logger):
    if not arquivo_path.exists():
        logger.error(f"[✗] Arquivo '{arquivo_path}' não encontrado.")
        return False

    try:
        markdown_dir = base_dir / "markdown"
        subprocess.check_call([
            sys.executable, "-m", "nbconvert",
            "--to", "markdown",
            "--output-dir", str(markdown_dir),
            str(arquivo_path)
        ])
        logger.info(f"[✓] Convertido para Markdown com sucesso: {arquivo_path.name}\\n")
        return True
    except subprocess.CalledProcessError as e:
        logger.exception(f"[✗] Erro durante a conversão de {arquivo_path.name}: {e}\\n")
        return False
'''
    (functions_dir / "conversao.py").write_text(conversao_code)
    logger.info("[+] Criado: conversao.py")

# ---------------------------------------------------------------------------------------------------------------------
def criar_pastas(base_dir: Path):
    for pasta in PASTAS:
        dir_path = base_dir / pasta
        if not dir_path.exists():
            dir_path.mkdir(parents=True)
            logger.info(f"[+] Criada pasta: {dir_path}")
            if pasta == ".functions":
                criar_funcoes_padrao(dir_path)
        else:
            logger.info(f"[=] Pasta já existe: {dir_path}")

# ---------------------------------------------------------------------------------------------------------------------
def main():
    base_dir = Path(__file__).resolve().parent

    # 1. Criar pastas e arquivos de função (sem depender de importações)
    criar_pastas(base_dir)  # Esta é a função local declarada antes

    # 2. Adicionar caminho de .functions ao sys.path
    sys.path.insert(0, str(base_dir / ".functions"))

    # 3. Agora sim, importar os módulos personalizados
    from log import configurar_logger
    from estrutura import criar_pastas as criar_pastas_dinamico
    from conversao import converte_to_md

    # 4. Configurar logger (após importação)
    global logger
    logger = configurar_logger(base_dir)

    logger.info("[OK] Biblioteca 'nbconvert' disponível.")
    logger.info("[OK] Iniciando conversão de notebooks para Markdown...")

    # Reexecuta criar_pastas com logging (agora usando a versão dinâmica)
    criar_pastas_dinamico(base_dir, PASTAS, logger)

    notebooks_dir = base_dir / "notebooks"
    notebooks = [f for f in notebooks_dir.glob("*.ipynb") if f.is_file()]

    if not notebooks:
        logger.warning("[!] Nenhum arquivo .ipynb encontrado em /notebooks")
        return

    total = len(notebooks)
    convertidos = 0
    falhas = 0

    for arquivo in notebooks:
        sucesso = converte_to_md(arquivo, base_dir, logger)
        if sucesso:
            convertidos += 1
        else:
            falhas += 1

    logger.info("========== RESUMO DA EXECUÇÃO ========")
    logger.info(f"Total de arquivos encontrados______: {total}")
    logger.info(f"Convertidos com sucesso____________: {convertidos}")
    logger.info(f"Falharam na conversão______________: {falhas}")
    logger.info("======================================\n")
    
    print()
    resposta = input("Deseja criar um arquivo Dit? (S/n): ").strip().lower()
    if resposta in ["s", "sim", ""]:
        dit_path = base_dir / "doc" / "notebooks.docx"
        with open(dit_path, "w") as dit_file:
            for arquivo in notebooks:
                dit_file.write(f"{arquivo.name}\n")
        logger.info(f"[+] Arquivo Dit criado: {dit_path}")
    else:
        logger.info("[=] Criação do arquivo Dit cancelada.")    


# ---------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        import nbconvert
    except ImportError:
        print("[ERRO] nbconvert não está instalado. Tentando instalar...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "nbconvert"])
        except Exception as install_error:
            print(f"[ERRO] Falha ao instalar nbconvert: {install_error}")
            sys.exit(1)

    main()

