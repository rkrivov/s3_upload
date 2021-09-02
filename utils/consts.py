import os
import sys

import psutil
from botocore.response import StreamingBody

APP_NAME = os.path.basename(os.path.dirname(sys.argv[0]))

HOME_FOLDER = os.environ.get('HOME')
WORK_FOLDER = os.path.dirname(sys.argv[0])
TEMP_FOLDER = os.path.join(HOME_FOLDER, '.tmp/')
LIB_FOLDER = os.path.join(HOME_FOLDER, 'Library/')
LIB_CACHES_FOLDER = os.path.join(LIB_FOLDER, 'Caches/')
CACHES_FOLDER = os.path.join(LIB_CACHES_FOLDER, 'com.drouland.s3.backup/')
CONTAINERS_FOLDER = os.path.join(LIB_FOLDER, 'Containers/')
DISK_O_FOLDER = os.path.join(CONTAINERS_FOLDER, 'Mail.Ru.DiskO.as/Data/Disk-O.as.mounts/')

FOLDERS = {
    'DISK-O': DISK_O_FOLDER,
    'CONTAINERS': CONTAINERS_FOLDER,
    'LIB': LIB_FOLDER,
    'HOME': HOME_FOLDER,
    'WORK': WORK_FOLDER,
    'TEMP': TEMP_FOLDER
}

SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 60 * SECONDS_PER_MINUTE
SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR
MSECONDS_PER_SECOND = 1000

CPU_COUNT = psutil.cpu_count(logical=False)
MAX_CONCURRENCY = (CPU_COUNT // 2) + 2

KBYTES = 1 << 10
MBYTES = KBYTES << 10
GBYTES = MBYTES << 10
TBYTES = GBYTES << 10
PBYTES = TBYTES << 10
EBYTES = PBYTES << 10
ZBYTES = EBYTES << 10
YBYTES = ZBYTES << 10

memory = None

FILE_SIZE_LIMIT = 32 * GBYTES
BUFFER_SIZE = 64 * MBYTES

StreamingBody._DEFAULT_CHUNK_SIZE = 64 * KBYTES

MAX_FILE_LOG_SIZE = 20 * MBYTES
MAX_LOG_BACKUP_COUNT = 5

THREADS_IN_POOL = (CPU_COUNT // 2) + 1
MAX_QUEQUE_SIZE = 7000  # CPU_COUNT * 2 + 1
THREAD_TIMEOUT_IN_MILLISECONDS = 60
THREAD_TIMEOUT = float(THREAD_TIMEOUT_IN_MILLISECONDS) / 1000.0

CLEAR_TO_END_LINE = "\033[K"

ENCODER = "utf-8"

MD5_ENCODER_NAME = "md5"
SHA256_ENCODER_NAME = "sha256"
SHA512_ENCODER_NAME = "sha512"

# Дополнительные свойства для текта:
BOLD = '\033[1m'  # ${BOLD}      # жирный шрифт (интенсивный цвет)

DBOLD = '\033[2m'  # ${DBOLD}    # полу яркий цвет (тёмно-серый, независимо от цвета)
NBOLD = '\033[22m'  # ${NBOLD}    # установить нормальную интенсивность
UNDERLINE = '\033[4m'  # ${UNDERLINE}  # подчеркивание
NUNDERLINE = '\033[4m'  # ${NUNDERLINE}  # отменить подчеркивание
BLINK = '\033[5m'  # ${BLINK}    # мигающий
NBLINK = '\033[5m'  # ${NBLINK}    # отменить мигание

INVERSE = '\033[7m'  # ${INVERSE}    # реверсия (знаки приобретают цвет фона, а фон -- цвет знаков)
NINVERSE = '\033[7m'  # ${NINVERSE}    # отменить реверсию
BREAK = '\033[m'  # ${BREAK}    # все атрибуты по умолчанию
NORMAL = '\033[0m'  # ${NORMAL}    # все атрибуты по умолчанию

# Цвет текста:
BLACK = '\033[0;30m'  # ${BLACK}    # чёрный цвет знаков
RED = '\033[0;31m'  # ${RED}      # красный цвет знаков
GREEN = '\033[0;32m'  # ${GREEN}    # зелёный цвет знаков
YELLOW = '\033[0;33m'  # ${YELLOW}    # желтый цвет знаков
BLUE = '\033[0;34m'  # ${BLUE}      # синий цвет знаков
MAGENTA = '\033[0;35m'  # ${MAGENTA}    # фиолетовый цвет знаков
CYAN = '\033[0;36m'  # ${CYAN}      # цвет морской волны знаков
GRAY = '\033[0;37m'  # ${GRAY}      # серый цвет знаков

# Цветом текста (жирным) (bold) :
DEF = '\033[0;39m'  # ${DEF}
DGRAY = '\033[1;30m'  # ${DGRAY}
LRED = '\033[1;31m'  # ${LRED}
LGREEN = '\033[1;32m'  # ${LGREEN}
LYELLOW = '\033[1;33m'  # ${LYELLOW}
LBLUE = '\033[1;34m'  # ${LBLUE}
LMAGENTA = '\033[1;35m'  # ${LMAGENTA}
LCYAN = '\033[1;36m'  # ${LCYAN}
WHITE = '\033[1;37m'  # ${WHITE}

# Цвет фона
BGBLACK = '\033[40m'  # ${BGBLACK}
BGRED = '\033[41m'  # ${BGRED}
BGGREEN = '\033[42m'  # ${BGGREEN}
BGBROWN = '\033[43m'  # ${BGBROWN}
BGBLUE = '\033[44m'  # ${BGBLUE}
BGMAGENTA = '\033[45m'  # ${BGMAGENTA}
BGCYAN = '\033[46m'  # ${BGCYAN}
BGGRAY = '\033[47m'  # ${BGGRAY}
BGDEF = '\033[49m'  # ${BGDEF}

RUS_TO_LAT = {
    'а': 'a',
    'б': 'b',
    'в': 'v',
    'г': 'g',
    'д': 'd',
    'е': 'e',
    'ё': 'yo',
    'ж': 'zh',
    'з': 'z',
    'и': 'i',
    'й': 'j',
    'к': 'k',
    'л': 'l',
    'м': 'm',
    'н': 'n',
    'о': 'o',
    'п': 'p',
    'р': 'r',
    'с': 's',
    'т': 't',
    'у': 'u',
    'ф': 'f',
    'х': 'kh',
    'ц': 'ts',
    'ч': 'tsch',
    'ш': 'sh',
    'щ': 'shch',
    'ъ': '``',
    'ы': 'y',
    'ь': '`',
    'э': 'e',
    'ю': 'yu',
    'я': 'ya'
}
