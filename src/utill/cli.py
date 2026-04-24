import subprocess

from ._lazy_logger import logger


def _cli(command: list[str], cwd: str | None = None, shell: bool = False):
    if shell:  # bash
        command_str = " ".join(command)
        process = subprocess.Popen(
            command_str,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            shell=True,
            executable="/bin/bash",
            cwd=cwd,
        )
    else:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=cwd,
        )

    for stdout_line in iter(process.stdout.readline, ""):
        yield stdout_line

    process.stdout.close()
    return_code = process.wait()
    if return_code != 0:
        logger.error(f"Command failed with return code {process.returncode}")
        raise subprocess.CalledProcessError(return_code, command)


def shell(command: list[str], cwd: str | None = None, print_stdout: bool = True) -> str:
    logger.info(f"Executing command: {' '.join(command)}")
    output: list[str] = []
    for res in _cli(command, cwd, shell=True):
        output.append(res)
        if print_stdout:
            logger.info(res.replace("\n", ""))
    return "".join(output)


def bash(command: list[str], cwd: str | None = None, print_stdout: bool = True) -> str:
    logger.info(f"Executing command: {' '.join(command)}")
    output: list[str] = []
    for res in _cli(command, cwd, shell=False):
        output.append(res)
        if print_stdout:
            logger.info(res.replace("\n", ""))
    return "".join(output)
