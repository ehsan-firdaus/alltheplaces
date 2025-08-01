import logging
import re
import time
from collections import defaultdict

DAYS = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
DAYS_FROM_SUNDAY = DAYS[-1:] + DAYS[:-1]
DAYS_3_LETTERS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
DAYS_3_LETTERS_FROM_SUNDAY = DAYS_3_LETTERS[-1:] + DAYS_3_LETTERS[:-1]
DAYS_FULL = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]
DAYS_WEEKDAY = ["Mo", "Tu", "We", "Th", "Fr"]
DAYS_WEEKEND = ["Sa", "Su"]

# The below DAYS dicts are provided to be used with sanitise_day in order for us to do best attempts at matching the
# given day into an English 2 char day to be used inside ATP and then to be exported to OSM formatted opening hours.

DAYS_AT = {
    "Mo": "Mo",
    "Montag": "Mo",
    "Di": "Tu",
    "Dienstag": "Tu",
    "Mi": "We",
    "Mittwoch": "We",
    "Do": "Th",
    "Donnerstag": "Th",
    "Fr": "Fr",
    "Freitag": "Fr",
    "Sa": "Sa",
    "Samstag": "Sa",
    "So": "Su",
    "Sonntag": "Su",
}

DAYS_EN = {
    "Monday": "Mo",
    "Mondays": "Mo",
    "Mon": "Mo",
    "Mo": "Mo",
    "Tuesday": "Tu",
    "Tuesdays": "Tu",
    "Tues": "Tu",
    "Tue": "Tu",
    "Tu": "Tu",
    "Wednesday": "We",
    "Wednesdays": "We",
    "Wednes": "We",
    "Weds": "We",
    "Wed": "We",
    "We": "We",
    "Thursday": "Th",
    "Thursdays": "Th",
    "Thu": "Th",
    "Thr": "Th",
    "Thur": "Th",
    "Thrs": "Th",
    "Thurs": "Th",
    "Th": "Th",
    "Friday": "Fr",
    "Fridays": "Fr",
    "Fri": "Fr",
    "Fr": "Fr",
    "Saturday": "Sa",
    "Saturdays": "Sa",
    "Satur": "Sa",
    "Sat": "Sa",
    "Sa": "Sa",
    "Sunday": "Su",
    "Sundays": "Su",
    "Sun": "Su",
    "Su": "Su",
}

DAYS_DE = {
    # "Feiertag": "PH",
    "Montag": "Mo",
    "Mo": "Mo",
    "Dienstag": "Tu",
    "Di": "Tu",
    "Mittwoch": "We",
    "Mi": "We",
    "Donnerstag": "Th",
    "Do": "Th",
    "Freitag": "Fr",
    "Fr": "Fr",
    "Samstag": "Sa",
    "Sa": "Sa",
    "Sonntag": "Su",
    "So": "Su",
}

DAYS_BG = {
    "Понеделник": "Mo",
    "Пн": "Mo",
    "Пон": "Mo",
    "По": "Mo",
    "Вторник": "Tu",
    "Вт": "Tu",
    "Сряда": "We",
    "Ср": "We",
    "Четвъртък": "Th",
    "Че": "Th",
    "Чт": "Th",
    "Четв": "Th",
    "Петък": "Fr",
    "Пет": "Fr",
    "Пе": "Fr",
    "Пт": "Fr",
    "Събота": "Sa",
    "Съб": "Sa",
    "Съ": "Sa",
    "Сб": "Sa",
    "Неделя": "Su",
    "Нед": "Su",
    "нед": "Su",
    "Не": "Su",
    "Нд": "Su",
}

DAYS_BR = {
    "Segunda": "Mo",
    "Seg": "Mo",
    "Terça": "Tu",
    "Ter": "Tu",
    "Quarta": "We",
    "Qua": "We",
    "Quinta": "Th",
    "Qui": "Th",
    "Sexta": "Fr",
    "Sex": "Fr",
    "Sábado": "Sa",
    "Sáb": "Sa",
    "Domingos": "Su",
    "Dom": "Su",
}

DAYS_CH = {
    "Mo": "Mo",
    "Di": "Tu",
    "Mi": "We",
    "Do": "Th",
    "Fr": "Fr",
    "Sa": "Sa",
    "So": "Su",
}

DAYS_CN = {
    # Standard Modern Chinese
    "月曜日": "Mo",
    "Yuèyàorì": "Mo",
    "星期一": "Mo",
    "Xīngqīyī": "Mo",
    "週一": "Mo",
    "Zhōuyī": "Mo",
    "禮拜一": "Mo",
    "Lǐbàiyī": "Mo",
    "周一": "Mo",
    "火曜日": "Tu",
    "Huǒyàorì": "Tu",
    "星期二": "Tu",
    "Xīngqī'èr": "Tu",
    "週二": "Tu",
    "Zhōu'èr": "Tu",
    "禮拜二": "Tu",
    "Lǐbài'èr": "Tu",
    "水曜日": "We",
    "Shuǐyàorì": "We",
    "星期三": "We",
    "Xīngqīsān": "We",
    "週三": "We",
    "Zhōusān": "We",
    "禮拜三": "We",
    "Lǐbàisān": "We",
    "木曜日": "Th",
    "Mùyàorì": "Th",
    "星期三": "We",
    "星期四": "Th",
    "Xīngqīsì": "Th",
    "週四": "Th",
    "Zhōusì": "Th",
    "禮拜四": "Th",
    "Lǐbàisì": "Th",
    "金曜日": "Fr",
    "Jīnyàorì": "Fr",
    "星期五": "Fr",
    "Xīngqīwǔ": "Fr",
    "週五": "Fr",
    "Zhōuwǔ": "Fr",
    "禮拜五": "Fr",
    "Lǐbàiwǔ": "Fr",
    "土曜日": "Sa",
    "Tǔyàorì": "Sa",
    "星期六": "Sa",
    "Xīngqīliù": "Sa",
    "週六": "Sa",
    "Zhōuliù": "Sa",
    "禮拜六": "Sa",
    "Lǐbàiliù": "Sa",
    "日曜日": "Su",
    "Rìyàorì": "Su",
    "星期日": "Su",
    "Xīngqīrì": "Su",
    "星期天": "Su",
    "Xīngqītiān": "Su",
    "週日": "Su",
    "Zhōurì": "Su",
    "週天": "Su",
    "Zhōutiān": "Su",
    "禮拜天": "Su",
    "Lǐbàitiān": "Su",
    "禮拜日": "Su",
    "Lǐbàirì": "Su",
    "周日": "Su",
}

DAYS_CZ = {
    "Pondělí": "Mo",
    "Po": "Mo",
    "Úterý": "Tu",
    "Út": "Tu",
    "Středa": "We",
    "St": "We",
    "Čtvrtek": "Th",
    "Čt": "Th",
    "Pátek": "Fr",
    "Pá": "Fr",
    "Sobota": "Sa",
    "So": "Sa",
    "Neděle": "Su",
    "Ne": "Su",
}

DAYS_GR = {
    "Δε": "Mo",
    "Δευτέρα": "Mo",
    "Τρ": "Tu",
    "Τρίτη": "Tu",
    "Τε": "We",
    "Τετάρτη": "We",
    "Πέ": "Th",
    "Πέμπτη": "Th",
    "Πα": "Fr",
    "Παρασκευή": "Fr",
    "Σά": "Sa",
    "Σάββατο": "Sa",
    "Κυ": "Su",
    "Κυριακή": "Su",
}

DAYS_HR = {
    "Ponedjeljak": "Mo",
    "Pon": "Mo",
    "Utorak": "Tu",
    "Srijeda": "We",
    "Četvrtak": "Th",
    "Čet": "Th",
    "Petak": "Fr",
    "Pet": "Fr",
    "Subota": "Sa",
    "Sub": "Sa",
    "Nedjelja": "Su",
    "Ned": "Su",
}

DAYS_HU = {
    "Hétfő": "Mo",
    "Hé": "Mo",
    "H": "Mo",
    "Kedd": "Tu",
    "Ke": "Tu",
    "K": "Tu",
    "Szerda": "We",
    "Sze": "We",
    # "Sz": "We",
    "Csütörtök": "Th",
    "Csü": "Th",
    "Cs": "Th",
    "Péntek": "Fr",
    "Pé": "Fr",
    "P": "Fr",
    "Szombat": "Sa",
    "Szo": "Sa",
    # "Sz": "Sa",
    "Va": "Sa",
    "V": "Su",
    "Vasárnap": "Su",
    "Vas": "Su",
}

DAYS_IL = {
    "יום שני": "Mo",
    "יום שלישי": "Tu",
    "יום רביעי": "We",
    "יום חמישי": "Th",
    "יום שישי": "Fr",
    "יום שבת": "Sa",
    "יום ראשון": "Su",
}

DAYS_KR = {
    "월요일": "Mo",
    "화요일": "Tu",
    "수요일": "We",
    "목요일": "Th",
    "금요일": "Fr",
    "토요일": "Sa",
    "일요일": "Su",
}

DAYS_LT = {
    "pirmadienis": "Mo",
    "antradienis": "Tu",
    "trečiadienis": "We",
    "ketvirtadienis": "Th",
    "penktadienis": "Fr",
    "šeštadienis": "Sa",
    "sekmadienis": "Su",
    "I": "Mo",
    "II": "Tu",
    "III": "We",
    "IV": "Th",
    "V": "Fr",
    "VI": "Sa",
    "VII": "Su",
    "Iv": "Th",
    "Vi": "Sa",
    "Vii": "Su",
}

DAYS_SE = {
    "Måndag": "Mo",
    "Mån": "Mo",
    "Tisdag": "Tu",
    "Tis": "Tu",
    "Onsdag": "We",
    "Ons": "We",
    "Torsdag": "Th",
    "Tors": "Th",
    "Fredag": "Fr",
    "Fre": "Fr",
    "Lördag": "Sa",
    "Lör": "Sa",
    "Söndag": "Su",
    "Sön": "Su",
}
DAYS_SI = {
    "Po": "Mo",
    "Pon": "Mo",
    "Ponedeljek": "Mo",
    "To": "Tu",
    "Tor": "Tu",
    "Torek": "Tu",
    "Sr": "We",
    "Sre": "We",
    "Sreda": "We",
    "Če": "Th",
    "Čet": "Th",
    "Četrtek": "Th",
    "Pe": "Fr",
    "Pet": "Fr",
    "Petek": "Fr",
    "So": "Sa",
    "Sob": "Sa",
    "Sobota": "Sa",
    "Ne": "Su",
    "Ned": "Su",
    "Nedelja": "Su",
}
DAYS_IT = {
    "Lunedì": "Mo",
    "Lunedi": "Mo",
    "Lun": "Mo",
    "Lun.": "Mo",
    "Lu": "Mo",
    "Lu.": "Mo",
    "Martedì": "Tu",
    "Martedi": "Tu",
    "Mar": "Tu",
    "Mar.": "Tu",
    "Ma": "Tu",
    "Ma.": "Tu",
    "Mercoledì": "We",
    "Mercoledi": "We",
    "Mer": "We",
    "Mer.": "We",
    "Me": "We",
    "Me.": "We",
    "Giovedì": "Th",
    "Giovedi": "Th",
    "Gio": "Th",
    "Gio.": "Th",
    "Gi": "Th",
    "Gi.": "Th",
    "Venerdì": "Fr",
    "Venerdi": "Fr",
    "Ven": "Fr",
    "Ven.": "Fr",
    "Ve": "Fr",
    "Ve.": "Fr",
    "Sabato": "Sa",
    "Sab": "Sa",
    "Sab.": "Sa",
    "Sa": "Sa",
    "Sa.": "Sa",
    "Domenica": "Su",
    "Domenicale": "Su",
    "Dom": "Su",
    "Dom.": "Su",
    "Do": "Su",
    "Do.": "Su",
}
DAYS_FR = {
    "Lu": "Mo",
    "Ma": "Tu",
    "Me": "We",
    "Je": "Th",
    "Ve": "Fr",
    "Sa": "Sa",
    "Di": "Su",
    "Lundi": "Mo",
    "Mardi": "Tu",
    "Mercredi": "We",
    "Jeudi": "Th",
    "Vendredi": "Fr",
    "Samedi": "Sa",
    "Dimanche": "Su",
}
DAYS_NL = {
    "Ma": "Mo",
    "Di": "Tu",
    "Wo": "We",
    "Do": "Th",
    "Vr": "Fr",
    "Za": "Sa",
    "Zo": "Su",
    "Maandag": "Mo",
    "Dinsdag": "Tu",
    "Woensdag": "We",
    "Donderdag": "Th",
    "Vrijdag": "Fr",
    "Zaterdag": "Sa",
    "Zondag": "Su",
}
DAYS_PL = {
    "Pn": "Mo",
    "Po": "Mo",
    "Pon": "Mo",
    "Pn.": "Mo",
    "Pon.": "Mo",
    "Poniedziałek": "Mo",
    "Wt": "Tu",
    "Wto": "Tu",
    "Wto.": "Tu",
    "Wtorek": "Tu",
    "Śr": "We",
    "Sr": "We",
    "Sro": "We",
    "Śro": "We",
    "Śro.": "We",
    "Środa": "We",
    "Cz": "Th",
    "Czw": "Th",
    "Czw.": "Th",
    "Czwartek": "Th",
    "Pt": "Fr",
    "Pt.": "Fr",
    "Pi": "Fr",
    "Pia": "Fr",
    "Piątek": "Fr",
    "Sb": "Sa",
    "So": "Sa",
    "Sob": "Sa",
    "Sob.": "Sa",
    "Sobota": "Sa",
    "Nd": "Su",
    "Ni": "Su",
    "Nie": "Su",
    "Ndz": "Su",
    "Niedz": "Su",
    "Niedzela": "Su",
    "Niedziela": "Su",
    "Niedziele": "Su",
}
DAYS_PT = {
    # "Feriado": "PH",
    # "Se": "Mo",
    "Segunda": "Mo",
    "Te": "Tu",
    "Terça": "Tu",
    # "Qu": "We",
    "Quarta": "We",
    # "Qu": "Th",
    "Quinta": "Th",
    # "Se": "Fr",
    "Sexta": "Fr",
    "Sábado": "Sa",
    "Sa": "Sa",
    "Sá": "Sa",
    "Sab": "Sa",
    "Sáb": "Sa",
    "Do": "Su",
    "Dom": "Su",
    "Domingo": "Su",
}
DAYS_SK = {
    "Po": "Mo",
    "Pondelok": "Mo",
    "Ut": "Tu",
    "Út": "Tu",
    "Utorok": "Tu",
    "Útorok": "Tu",
    "St": "We",
    "Streda": "We",
    "Št": "Th",
    "Štvrtok": "Th",
    "Stvrtok": "Th",
    "Pi": "Fr",
    "Piatok": "Fr",
    "So": "Sa",
    "Sobota": "Sa",
    "Ne": "Su",
    "Nedeľa": "Su",
    "Nedela": "Su",
}
DAYS_RU = {
    "Пн": "Mo",
    "Понедельник": "Mo",
    "Вт": "Tu",
    "Вторник": "Tu",
    "Ср": "We",
    "Среда": "We",
    "Среду": "We",
    "Чт": "Th",
    "Четверг": "Th",
    "Пт": "Fr",
    "Пятница": "Fr",
    "Пятницу": "Fr",
    "Сб": "Sa",
    "Суббота": "Sa",
    "Субботу": "Sa",
    "Вс": "Su",
    "Воскресенье": "Su",
}
DAYS_RS = {
    "Ponedeljak": "Mo",
    "Utorak": "Tu",
    "Sreda": "We",
    "Četvrtak": "Th",
    "Petak": "Fr",
    "Subota": "Sa",
    "Nedelja": "Su",
}
DAYS_NO = {
    "Mandag": "Mo",
    "Måndag": "Mo",
    "Man": "Mo",
    "Tirsdag": "Tu",
    "Tysdag": "Tu",
    "Onsdag": "We",
    "Torsdag": "Th",
    "Tors": "Th",
    "Fredag": "Fr",
    "Fre": "Fr",
    "Lørdag": "Sa",
    "Laurdag": "Sa",
    "Lør": "Sa",
    "Søndag": "Su",
    "Sundag": "Su",
    "Søn": "Su",
}
DAYS_DK = {
    "Mandag": "Mo",
    "Man": "Mo",
    "Ma": "Mo",
    "Tirsdag": "Tu",
    "Ti": "Tu",
    "Onsdag": "We",
    "On": "We",
    "Torsdag": "Th",
    "Tors": "Th",
    "To": "Th",
    "Fredag": "Fr",
    "Fre": "Fr",
    "Fr": "Fr",
    "Lørdag": "Sa",
    "Lør": "Sa",
    "Lø": "Sa",
    "Søndag": "Su",
    "Søn": "Su",
    "So": "Su",
}
DAYS_FI = {
    "Maanantai": "Mo",
    "Ma": "Mo",
    "Tiistai": "Tu",
    "Ti": "Tu",
    "Keskiviikko": "We",
    "Ke": "We",
    "Torstai": "Th",
    "To": "Th",
    "Perjantai": "Fr",
    "Pe": "Fr",
    "Lauantai": "Sa",
    "La": "Sa",
    "Sunnuntai": "Su",
    "Su": "Su",
}
DAYS_ES = {
    "Lunes": "Mo",
    "Lun": "Mo",
    "Lu": "Mo",
    "Martes": "Tu",
    "Mar": "Tu",
    "Ma": "Tu",
    "Miercoles": "We",
    "Miércoles": "We",
    "Mie": "We",
    "Mié": "We",
    "Mi": "We",
    "Jueves": "Th",
    "Jue": "Th",
    "Ju": "Th",
    "Viernes": "Fr",
    "Vie": "Fr",
    "Vi": "Fr",
    "Sabado": "Sa",
    "Sábado": "Sa",
    "Sábados": "Sa",
    "Sab": "Sa",
    "Sáb": "Sa",
    "Sa": "Sa",
    "Domingo": "Su",
    "Domingos": "Su",
    "Dom": "Su",
    "Do": "Su",
}

DAYS_RO = {
    "Luni": "Mo",
    "Marți": "Tu",
    "Miercuri": "We",
    "Joi": "Th",
    "Vineri": "Fr",
    "Sâmbătă": "Sa",
    "Duminică": "Su",
}

DAYS_SR = {
    "Pon": "Mo",
    "Ponedeljak": "Mo",
    "Ponediljak": "Mo",
    "Ponedjeljak": "Mo",
    "Понедељак": "Mo",
    "Понеделник": "Mo",
    "Uto": "Tu",
    "Utorak": "Tu",
    "Уторак": "Tu",
    "Sri": "We",
    "Sreda": "We",
    "Среда": "We",
    "Cet": "Th",
    "Čet": "Th",
    "Četvrtak": "Th",
    "Cetvrtak": "Th",
    "Четвртак": "Th",
    "Pet": "Fr",
    "Petak": "Fr",
    "Петак": "Fr",
    "Sub": "Sa",
    "Subota": "Sa",
    "Субота": "Sa",
    "Ned": "Su",
    "Nedelja": "Su",
    "Недеља": "Su",
}

DAYS_TR = {
    "Pazartesi": "Mo",
    "Salı": "Tu",
    "Çarşamba": "We",
    "Perşembe": "Th",
    "Cuma": "Fr",
    "Cumartesi": "Sa",
    "Pazar": "Su",
}

DAYS_ID = {
    "Senin": "Mo",
    "Selasa": "Tu",
    "Rabu": "We",
    "Kamis": "Th",
    "Jumat": "Fr",
    "Sabtu": "Sa",
    "Minggu": "Su",
    "Ahad": "Su",
}

DAYS_TH = {
    "วันจันทร์": "Mo",
    "จันทร์": "Mo",
    "วันอังคาร": "Tu",
    "วันพุธ": "We",
    "วันพฤหัสบดี": "Th",
    "วันศุกร์": "Fr",
    "วันเสาร์": "Sa",
    "เสาร์": "Sa",
    "วันอาทิตย์": "Su",
}

# See https://github.com/alltheplaces/alltheplaces/issues/7360
# A list ordered by languages most frequently used for web content as of January 2024, by share of websites.
# See WPStoreLocator for example usage.
DAYS_BY_FREQUENCY = [
    DAYS_EN,
    DAYS_ES,
    DAYS_DE,
    DAYS_RU,
    # Japanese missing
    DAYS_FR,
    DAYS_PT,
    DAYS_IT,
    DAYS_TR,
    DAYS_DK,
    DAYS_PL,
    # Persian
    DAYS_CZ,
    # And everything else
    DAYS_AT,
    DAYS_BG,
    DAYS_CH,
    DAYS_FI,
    DAYS_GR,
    DAYS_HR,
    DAYS_HU,
    DAYS_IL,
    DAYS_NL,
    DAYS_NO,
    DAYS_RO,
    DAYS_RS,
    DAYS_SE,
    DAYS_SI,
    DAYS_SK,
    DAYS_SR,
]

NAMED_DAY_RANGES_DK = {
    "Hverdage": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],  # Weekdays
}

NAMED_DAY_RANGES_EN = {
    "Daily": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "All Days": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "Weekday": ["Mo", "Tu", "We", "Th", "Fr"],
    "Weekdays": ["Mo", "Tu", "We", "Th", "Fr"],
    "Weekend": ["Sa", "Su"],
    "Weekends": ["Sa", "Su"],
}

NAMED_TIMES_EN = {
    "Midday": ["12:00PM", "12:00"],
    "Midnight": ["12:00AM", "00:00"],
}

NAMED_DAY_RANGES_IT = {
    "Ogni Giorno": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "Tutti I Giorni": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "Giorni Lavorativi": ["Mo", "Tu", "We", "Th", "Fr"],
    "Lavorativi": ["Mo", "Tu", "We", "Th", "Fr"],
    "Giorni Feriali": ["Mo", "Tu", "We", "Th", "Fr"],
    "Feriali": ["Mo", "Tu", "We", "Th", "Fr"],
    "Weekend": ["Sa", "Su"],
    "Weekends": ["Sa", "Su"],
    "Prefestivi": ["Sa"],
    "Prefestivo": ["Sa"],
}

NAMED_TIMES_IT = {
    "Mezzogiorno": ["12:00PM", "12:00"],
    "Mezzanotte": ["12:00AM", "00:00"],
}

NAMED_DAY_RANGES_RU = {
    "Ежедневно": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],  # Daily
    "Eжедн": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],  # Daily
    "По Будням": ["Mo", "Tu", "We", "Th", "Fr"],  # Weekdays
    "По Выходным": ["Sa", "Su"],  # Weekends
}

NAMED_TIMES_RU = {
    "Круглосуточно": ["00:00", "23:59"],  # 24/7
}

NAMED_DAY_RANGES_KR = {
    "연중무휴": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "연중무": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "중무휴": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
    "연중": ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"],
}

DELIMITERS_EN = [
    "-",
    "–",
    "—",
    "―",
    "‒",
    "to",
    "and",
    "from",
    "thru",
    "through",
    "until",
]

DELIMITERS_IT = [
    ",",
    "-",
    "–",
    "—",
    "―",
    "‒",
    "/",
    "e",
    "dal",
    "al",
    "il",
    "fino al",
    "alle",
    "alla",
    "fino alle",
    "dalle",
    "dalle ore",
    "da",
    "a",
    "dall'",
    "all'",
    "aperti",
    "aperto",
    "apre",
    "apriamo",
]

DELIMITERS_DE = ["-", "–", "—", "―", "‒", "bis"]

DELIMITERS_ES = [
    "-",
    "a",
    "y",
    "de",
]

DELIMITERS_FR = ["-", "–", "—", "―", "‒", "au", "à", "de"]

DELIMITERS_PT = [
    "-",
    "–",
    "—",
    "―",
    "‒",
    "a",
    "das",
    "às",
    "de",
]

DELIMITERS_PL = [
    "-",
    "–",
    "—",
    "―",
    "‒",
    "od",
    "do",
]

DELIMITERS_RU = DELIMITERS_EN + ["с", "по", "до", "в", "во"]

DELIMITERS_KR = DELIMITERS_EN + ["~"]

CLOSED_AT = ["geschlossen"]

CLOSED_EN = ["closed", "off"]

CLOSED_DE = ["geschlossen"]

CLOSED_IT = ["chiuso", "chiusi", "siamo chiusi"]

CLOSED_NL = ["gesloten"]

CLOSED_TH = ["ปิดทำการ"]

logger = logging.getLogger(__name__)


def day_range(start_day, end_day):
    start_ix = DAYS.index(sanitise_day(start_day))
    end_ix = DAYS.index(sanitise_day(end_day))
    if start_ix <= end_ix:
        return DAYS[start_ix : end_ix + 1]
    else:
        return DAYS[start_ix:] + DAYS[: end_ix + 1]


def sanitise_day(day: str, days: {} = DAYS_EN) -> str:
    if day is None:
        return None

    day = day.strip("-.\t ").lower()

    day = day.replace("https://", "").replace("http://", "").replace("schema.org/", "").title()

    if "#" in day:
        day = day.split("#", 1)[1]

    return days.get(day)


class OpeningHours:
    def __init__(self):
        self.day_hours = defaultdict(set)
        self.days_closed = set()

    def __bool__(self):
        return bool(self.day_hours or self.days_closed)

    def add_days_range(self, days: [str], open_time, close_time, time_format="%H:%M"):
        for day in days:
            self.add_range(day, open_time, close_time, time_format=time_format)

    def set_closed(self, days: str | list[str]):
        """
        Mark days where the location has stated they are closed; as opposed to simply not provided or known from survey hours.

        This matches the syntax of https://wiki.openstreetmap.org/wiki/Key:opening_hours.
        Use of this in All the Places is more common than in OpenStreetMap where use of 'off' or 'closed'
        is quite uncommon.

        Recommended to use with an appropriate helper to pass a specific string or list, ie:
        `set_closed(DAYS_SR["Ponedeljak"])` or `set_closed(["Mo", "Tu", "We"])`

        In the situation where source data does not explicitly state a feature
        is closed on a specific day of a week, do not make an assumption of
        closure status in the absence of other data. For example, source data
        may be `{"open_days": ["monday", "wednesday"]}` for one feature, and
        `{"open_days": ["monday", "tuesday", "wednesday"]}` for a different
        feature. Do not assume that the first feature is closed on Tuesdays
        unless there is other data explicitly stating this is the case. If the
        front end website displaying this data treats the absence of `"Tuesday"`
        in the `"open_days"` array as closure and visually states "Tuesdays:
        closed" on the website, this function can then be used to set Tuesdays
        to be closed.
        """
        for day in [days] if isinstance(days, str) else days:
            day = sanitise_day(day)

            if day not in DAYS:
                raise ValueError(f"day must be one of {DAYS}, not {day!r}")
            self.day_hours.pop(day, None)
            self.days_closed.add(day)

    def add_range(self, day, open_time, close_time, time_format="%H:%M", closed=CLOSED_EN):
        day = sanitise_day(day)

        if day not in DAYS:
            raise ValueError(f"day must be one of {DAYS}, not {day!r}")

        if not open_time or not close_time:
            return
        if (
            isinstance(open_time, str)
            and isinstance(close_time, str)
            and open_time.lower() in closed
            and close_time.lower() in closed
        ):
            self.day_hours.pop(day, None)
            self.days_closed.add(day)
            return
        if isinstance(open_time, str):
            if open_time.lower() in closed:
                return
        if isinstance(close_time, str):
            if close_time.lower() in closed:
                return
            if open_time in ("00:00", "0:00", "00:00:00") and close_time in ("00:00", "0:00", "00:00:00"):
                # Invalid range supplied. Probably the source data uses this
                # notation for closed days.
                return
            if close_time in ("24:00", "00:00", "0:00"):
                close_time = "23:59"
            if close_time in ("24:00:00", "00:00:00"):
                close_time = "23:59:00"
        if not isinstance(open_time, time.struct_time):
            open_time = time.strptime(open_time, time_format)
        if not isinstance(close_time, time.struct_time):
            close_time = time.strptime(close_time, time_format)
            if close_time.tm_hour == 0 and close_time.tm_min == 0:
                # weird format not caught by checks above
                # may be 0:00 or even more divergent if time_format
                # parameter was used with some exotic value
                close_time = time.strptime("23:59", "%H:%M")
        if open_time.tm_hour == close_time.tm_hour and open_time.tm_min == close_time.tm_min:
            # A single time of day was provided, not a range. Ignore request.
            # Sometimes source data uses 00:00-00:00 as a range denoting a
            # closed day.
            return

        self.days_closed.discard(day)
        self.day_hours[day].add((open_time, close_time))

    def as_opening_hours(self) -> str:
        day_groups = []
        this_day_group = None

        # usually ; works fine as a separator between different days
        # but it has an annoying quirk, in OpenStreetMap opening_hours syntax
        # it overrides previous definitions
        # so for example
        # Mo-Sa 10:00-02:00; Su 09:00-02:00
        # means that object is closed on midnight between Saturday and Sunday
        # Mo-Sa 10:00-02:00; Su 00:00-02:00,09:00-02:00
        # in turn means that object is open also during early Sunday hours
        #
        # All the Places is using a small section of entire opening_hours syntax
        # so we need only check whether time goes over midnight and split it
        # in two regular ranges
        day_hours_midnight_split = defaultdict(set)
        time_format = "%H:%M"
        midnight_end = time.strptime("23:59", time_format)
        midnight_start = time.strptime("00:00", time_format)
        for index, day in enumerate(DAYS):
            for h in self.day_hours[day]:
                if h[0].tm_hour * 60 + h[0].tm_min > h[1].tm_hour * 60 + h[1].tm_min:
                    # start hour is greater than end hour, indicating that it is
                    # an over-midnight range
                    day_hours_midnight_split[day].add((h[0], midnight_end))
                    next_day = DAYS[(index + 1) % len(DAYS)]
                    day_hours_midnight_split[next_day].add((midnight_start, h[1]))
                    if next_day in self.days_closed:
                        self.days_closed.remove(next_day)
                else:
                    day_hours_midnight_split[day].add(h)
        for day in DAYS:
            if day in self.days_closed:
                hours = "closed"
            else:
                hours = ",".join(
                    "%s-%s"
                    % (
                        time.strftime("%H:%M", h[0]),
                        time.strftime("%H:%M", h[1]).replace("23:59", "24:00"),
                    )
                    for h in sorted(day_hours_midnight_split[day])
                )

            if not this_day_group:
                this_day_group = {"from_day": day, "to_day": day, "hours": hours}
            elif this_day_group["hours"] != hours:
                day_groups.append(this_day_group)
                this_day_group = {"from_day": day, "to_day": day, "hours": hours}
            elif this_day_group["hours"] == hours:
                this_day_group["to_day"] = day

        day_groups.append(this_day_group)

        opening_hours = ""

        for day_group in day_groups:
            if not day_group["hours"]:
                continue
            elif day_group["from_day"] == day_group["to_day"]:
                opening_hours += "{from_day} {hours}; ".format(**day_group)
            elif day_group["from_day"] == "Su" and day_group["to_day"] == "Sa":
                opening_hours += "{hours}; ".format(**day_group)
            else:
                opening_hours += "{from_day}-{to_day} {hours}; ".format(**day_group)
        opening_hours = opening_hours[:-2]

        return opening_hours

    @staticmethod
    def delimiters_regex(delimiters: list[str] = DELIMITERS_EN) -> str:
        """
        Creates a regular expression for capturing delimiters within
        a string containing time information. For example, in the
        string of "Monday: 10am-2pm", ":" and "-" are both
        delimiters.
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :returns: regular expression which captures delimiters in a
                  string containing opening time information.
        """
        delimiter_regex = r"\s*(?:"
        delimiter_regex_parts = []
        for delimiter in delimiters:
            delimiter_regex_parts.append(re.escape(delimiter))
        delimiter_regex = delimiter_regex + r"|".join(delimiter_regex_parts) + r")\s*"
        return delimiter_regex

    @staticmethod
    def single_days_regex(days: dict = DAYS_EN) -> str:
        """
        Creates a regular expression for capturing single day names
        within a string containing time information. For example, in the
        string of "Monday: 9am-5pm", "Monday" is captured.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :returns: regular expression which captures single day names
                  in a string containing opening time information.
        """
        single_days_regex = r"(?<!\w)(" + r"|".join(map(re.escape, days.keys())) + r")(?!\w)"
        return single_days_regex

    @staticmethod
    def day_ranges_regex(days: dict = DAYS_EN, delimiters: list[str] = DELIMITERS_EN) -> list[str]:
        """
        Creates a list of regular expressions for capturing all
        combinations of day ranges, with wrap-around for ranges
        which wrap around both Monday and Sunday as the first days
        of a week. For example, a regular expression is created for
        capturing strings of "Mon-Sun", "Sun to Sat", "Tue and Wed".
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :returns: list of regular expressions which capture day
                  ranges in a given string containing opening time
                  information.
        """
        # For each day of the week, create a list of synonyms.
        day_synonyms = defaultdict(list)
        for synonym, day in sorted(days.items()):
            day_synonyms[day].append(re.escape(synonym))

        # Assuming a week starts on Monday (most opening hours are
        # listed as such due to business vs. weekend hours), create
        # a regular expression of all day ranges possible in the
        # week starting Monday (or later).
        days_regex_parts = []
        for i in range(0, 6, 1):
            start_day_string = r"(?<!\w)(" + r"|".join(day_synonyms[DAYS[i]]) + r")(?!\w)"
            end_days = []
            for j in range(i + 1, 7, 1):
                end_days.append("|".join(day_synonyms[DAYS[j]]))
            end_days_string = r"(?<!\w)(" + r"|".join(end_days) + r")(?!\w)"
            days_regex_parts.append(start_day_string + OpeningHours.delimiters_regex(delimiters) + end_days_string)

        # Some sources will have a week start on Sunday and will
        # express day ranges as Sunday to Thursday (for example).
        # Create a regular expression to capture these day ranges
        # that commence on Sunday.
        start_day_string = r"(?<!\w)(" + r"|".join(day_synonyms["Su"]) + r")(?!\w)"
        end_days = []
        for j in range(0, 6, 1):
            end_days.append("|".join(day_synonyms[DAYS[j]]))
        end_days_string = r"(?<!\w)(" + r"|".join(end_days) + r")(?!\w)"
        days_regex_parts.append(start_day_string + OpeningHours.delimiters_regex(delimiters) + end_days_string)

        return days_regex_parts

    @staticmethod
    def named_day_ranges_regex(named_day_ranges: dict = NAMED_DAY_RANGES_EN) -> str:
        """
        Creates a regular expression for capturing named day ranges
        within a string containing time information. For example, in
        the string of "Weekends: 9am-5pm", "Weekends" is captured.
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :returns: regular expression which captures named day ranges
                  in a string containing opening time information.
        """
        named_day_ranges_regex = r"(?<!\w)(" + r"|".join(map(re.escape, named_day_ranges.keys())) + r")(?!\w)"
        return named_day_ranges_regex

    @staticmethod
    def any_day_extraction_regex(
        days: dict = DAYS_EN,
        named_day_ranges: dict = NAMED_DAY_RANGES_EN,
        delimiters: list[str] = DELIMITERS_EN,
    ) -> str:
        """
        Creates a regular expression for capturing any day formatted
        as single, range or named range with requested localisation.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :returns: regular expression which can extract day
                  information from a string.
        """
        # Create a regular expression for capturing day names and
        # day ranges from within a string containing opening time
        # information. This regular expression is designed to
        # capture the following types of strings:
        #   Mon-Wed
        #   Tuesday to Thursday
        #   Saturday
        #   Weekends
        days_regex = r"(?:"
        days_regex_parts = OpeningHours.day_ranges_regex(days=days, delimiters=delimiters)
        days_regex_parts.append(OpeningHours.named_day_ranges_regex(named_day_ranges=named_day_ranges))
        days_regex_parts.append(OpeningHours.single_days_regex(days=days))
        days_regex = days_regex + r"|".join(days_regex_parts) + r")"

        return days_regex

    @staticmethod
    def replace_named_times(hours_string: str, named_times: dict = NAMED_TIMES_EN, time_24h: bool = True) -> str:
        """
        Replaces named times (e.g. Midnight) in a string with their
        12h equivalent (e.g. 12:00AM) or 24h equivalent (e.g 00:00).
        :param hours_string: string of opening hours information
                             containing named times.
        :param named_times: dictionary mapping localised named times
                            of day to a tuple of equivalent 24hr time
                            and 12hr time respectively.
        :param time_24h: boolean for whether the resulting string
                         should have named times replaced with 24h
                         or 12h time ranges.
        :returns: string with named times replaced with their 24h
                  equivalent or 12h equivalent times.
        """
        replaced_hours_string = hours_string
        if time_24h is True:
            for named_time_name, named_time_time in named_times.items():
                replaced_hours_string = (
                    replaced_hours_string.replace(named_time_name.lower(), named_time_time[1])
                    .replace(named_time_name.title(), named_time_time[1])
                    .replace(named_time_name.upper(), named_time_time[1])
                )
        else:
            for named_time_name, named_time_time in named_times.items():
                replaced_hours_string = (
                    replaced_hours_string.replace(named_time_name.lower(), named_time_time[0])
                    .replace(named_time_name.title(), named_time_time[0])
                    .replace(named_time_name.upper(), named_time_time[0])
                )
        return replaced_hours_string

    @staticmethod
    def time_of_day_regex(time_24h: bool = True) -> str:
        """
        Creates a regular expression for capturing times of day in
        either 24h or 12h notation.
        :param time_24h: boolean for whether the resulting regular
                         expression should capture 24h or 12h
                         opening time information.
        :returns: regular expression which captures times of day in
                  a string.
        """
        if time_24h is True:
            # Regular expression for extracting 24h times (e.g. 15:45)
            time_regex = (
                r"(?<!\d)(\d(?!\d)|[01]\d|2[0-4])(?:(?:[:\.]?([0-5]\d))(?:[:\.]?[0-5]\d)?)?(?!(?:\d|:|[AP]\.?M\.?))"
            )
        else:
            # Regular expression for extracting 12h times (e.g. 9:30AM)
            time_regex = r"(?<!\d)(\d(?!\d)|0\d|1[012])(?:(?:[:\.]?([0-5]\d))(?:[:\.]?[0-5]\d)?)?\s*([AP]\.?M\.?)?(?!(?:\d|:|[AP]\.?M\.?))"
        return time_regex

    @staticmethod
    def hours_extraction_regex(
        time_24h: bool = True,
        days: dict = DAYS_EN,
        named_day_ranges: dict = NAMED_DAY_RANGES_EN,
        delimiters: list[str] = DELIMITERS_EN,
    ) -> str:
        """
        Creates a regular expression for capturing opening time
        information from a localised string.
        :param time_24h: boolean for whether the resulting regular
                         expression should capture 24h or 12h
                         opening time information.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :returns: regular expression which can extract opening time
                  information from a string.
        """
        days_regex = OpeningHours.any_day_extraction_regex(
            days=days, delimiters=delimiters, named_day_ranges=named_day_ranges
        )

        full_regex = (
            days_regex
            + r"(?:\W+|"
            + OpeningHours.delimiters_regex(delimiters)
            + r")((?:(?:\s*(?:,|/|-)?\s*)?"
            + OpeningHours.time_of_day_regex(time_24h=time_24h)
            + OpeningHours.delimiters_regex(delimiters)
            + OpeningHours.time_of_day_regex(time_24h=time_24h)
            + r")+)"
        )
        return full_regex

    @staticmethod
    def closed_days_extraction_regex(
        days: dict = DAYS_EN,
        named_day_ranges: dict = NAMED_DAY_RANGES_EN,
        delimiters: list[str] = DELIMITERS_EN,
        closed: list[str] = CLOSED_EN,
    ) -> str:
        """
        Creates a regular expression for capturing closed days
        information from a localised string.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :param closed: list of strings representing
                           localised meaning of "closed"
        :returns: regular expression which can extract opening time
                  information from a string.
        """
        days_regex = OpeningHours.any_day_extraction_regex(
            days=days, delimiters=delimiters, named_day_ranges=named_day_ranges
        )

        full_regex = (
            days_regex + r"(?:\W+|" + OpeningHours.delimiters_regex(delimiters) + r")((?:" + r"|".join(closed) + r"))"
        )
        return full_regex

    @staticmethod
    def days_in_day_range(
        day_range: list[str], days: dict = DAYS_EN, named_day_ranges: dict = NAMED_DAY_RANGES_EN
    ) -> list[str]:
        """ """
        day_list = []
        if len(day_range) == 1 or days[day_range[0].title()] == days[day_range[1].title()]:
            if day_range[0].title() in named_day_ranges.keys():
                day_list = named_day_ranges[day_range[0].title()]
            else:
                day_list = [days[day_range[0].title()]]
        elif len(day_range) == 2:
            start_day_index = DAYS.index(days[day_range[0].title()])
            end_day_index = DAYS.index(days[day_range[1].title()])
            if start_day_index > end_day_index:
                day_list = DAYS[start_day_index:] + DAYS[: end_day_index + 1]
            else:
                day_list = DAYS[start_day_index : end_day_index + 1]
        return day_list

    @staticmethod
    def extract_hours_from_string(
        ranges_string: str,
        days: dict = DAYS_EN,
        named_day_ranges: dict = NAMED_DAY_RANGES_EN,
        named_times: dict = NAMED_TIMES_EN,
        delimiters: list[str] = DELIMITERS_EN,
        closed: list[str] = CLOSED_EN,
    ) -> list[tuple]:
        """
        Extracts opening time information from a localised string.
        For every day or day range in the localised string with an
        opening hours range supplied, this function will return a
        tuple containing a day/day range, an opening time, and
        closing time, all in a normalised format.
        :param ranges_string: localised string containing opening
                              time information.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :param named_times: dictionary mapping localised named times
                            of day to a tuple of equivalent 24hr time
                            and 12hr time respectively.
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :returns: list of tuples where each tuple has a list of days
                  at each end of a day range (e.g. ["Mon", "Sat"] or
                  a list containing a single ["Weekday"], an opening time in 24h notation and a
                  closing time in 24h notation.
        :param closed: list of strings representing
                           localised meaning of "closed"
        """
        # Create regular expressions for extracting opening time information from a string.
        hours_extraction_regex_24h = OpeningHours.hours_extraction_regex(
            time_24h=True, days=days, named_day_ranges=named_day_ranges, delimiters=delimiters
        )
        hours_extraction_regex_12h = OpeningHours.hours_extraction_regex(
            time_24h=False, days=days, named_day_ranges=named_day_ranges, delimiters=delimiters
        )
        closed_days_extraction_regex = OpeningHours.closed_days_extraction_regex(
            days=days, named_day_ranges=named_day_ranges, delimiters=delimiters, closed=closed
        )

        # Replace named times in source ranges string (e.g. midnight -> 00:00).
        ranges_string_24h = OpeningHours.replace_named_times(ranges_string, named_times, True)
        ranges_string_12h = OpeningHours.replace_named_times(ranges_string, named_times, False)

        # Execute regular expressions.
        if re.search(r"\d\s*[AP]\.?M\.?", ranges_string_24h, re.IGNORECASE):
            # Input string contains AM/PM (or derivatives) and therefore
            # should be treated as having 12h time format. Execute the regular
            # expression for 12h time format only.
            results_24h = []
            results_12h = re.findall(hours_extraction_regex_12h, ranges_string_12h, re.IGNORECASE)
        else:
            # Execute the regular expression for 24h time format only. There
            # is an unlikely chance that the string is actually in 12h time
            # format and doesn't use AM/PM (or derivatives). Unfortunately
            # there is no way to disambiguate whether "Mo-Su 7:00-11:00" is a
            # feature open for 5 hours of the day, or 18 hours of the day. It
            # is however more likely that a feature is open in the morning and
            # afternoon, so we guess 24h time format. A spider will have to
            # provide a hint (such as adding "AM" after instances of ":00") if
            # this assumption of 24h time format is invalid for a particular
            # spider.
            results_24h = re.findall(hours_extraction_regex_24h, ranges_string_24h, re.IGNORECASE)
            results_12h = []
        results_closed = re.findall(closed_days_extraction_regex, ranges_string_24h, re.IGNORECASE)

        # Normalise results to 24h time.
        results = []
        if len(results_24h) > 0:
            # Parse 24h opening hour information.
            for result in results_24h:
                time_start_index = result.index(next(filter(lambda x: len(x) > 0 and x[0].isdigit(), result)))
                day_range = list(filter(None, result[:time_start_index]))
                days_in_range = OpeningHours.days_in_day_range(
                    day_range=day_range, days=days, named_day_ranges=named_day_ranges
                )
                time_ranges = re.findall(
                    OpeningHours.time_of_day_regex(time_24h=True)
                    + OpeningHours.delimiters_regex(delimiters)
                    + OpeningHours.time_of_day_regex(time_24h=True),
                    result[time_start_index],
                    re.IGNORECASE,
                )
                for time_range in time_ranges:
                    time_start_minute = time_range[1]
                    if not time_range[1]:
                        time_start_minute = "00"
                    time_end_minute = time_range[3]
                    if not time_range[3]:
                        time_end_minute = "00"
                    results.append(
                        (days_in_range, f"{time_range[0]}:{time_start_minute}", f"{time_range[2]}:{time_end_minute}")
                    )
        elif len(results_12h) > 0:
            # Parse 12h opening hour information.
            for result in results_12h:
                time_start_index = result.index(next(filter(lambda x: len(x) > 0 and x[0].isdigit(), result)))
                day_range = list(filter(None, result[:time_start_index]))
                days_in_range = OpeningHours.days_in_day_range(
                    day_range=day_range, days=days, named_day_ranges=named_day_ranges
                )
                time_ranges = re.findall(
                    OpeningHours.time_of_day_regex(time_24h=False)
                    + OpeningHours.delimiters_regex(delimiters)
                    + OpeningHours.time_of_day_regex(time_24h=False),
                    result[time_start_index],
                    re.IGNORECASE,
                )
                for time_range in time_ranges:
                    time_start_hour = time_range[0]
                    if time_start_hour == "00" or time_start_hour == "0":
                        time_start_hour = "12"
                    if time_range[1]:
                        time_start = f"{time_start_hour}:{time_range[1]}"
                    else:
                        time_start = f"{time_start_hour}:00"
                    if time_range[2]:
                        time_start = f"{time_start}{time_range[2].upper()}".replace(".", "")
                    else:
                        # If AM/PM is not specified, it is almost always going to be AM for start times.
                        time_start = f"{time_start}AM"
                    time_start_24h = time.strptime(time_start, "%I:%M%p")
                    time_start_24h = time.strftime("%H:%M", time_start_24h)
                    time_end_hour = time_range[3]
                    if time_end_hour == "00" or time_end_hour == "0":
                        time_end_hour = "12"
                    if time_range[4]:
                        time_end = f"{time_end_hour}:{time_range[4]}"
                    else:
                        time_end = f"{time_end_hour}:00"
                    if time_range[5]:
                        time_end = f"{time_end}{time_range[5].upper()}".replace(".", "")
                    else:
                        # If AM/PM is not specified, it is almost always going to be PM for end times.
                        time_end = f"{time_end}PM"
                    time_end_24h = time.strptime(time_end, "%I:%M%p")
                    time_end_24h = time.strftime("%H:%M", time_end_24h)
                    results.append((days_in_range, time_start_24h, time_end_24h))
        if len(results_closed) > 0:
            for result in results_closed:
                closed_index = result.index(next(filter(lambda x: len(x) > 0 and x.lower() in closed, result)))
                day_range = list(filter(None, result[:closed_index]))
                days_in_range = OpeningHours.days_in_day_range(
                    day_range=day_range, days=days, named_day_ranges=named_day_ranges
                )
                results.append((days_in_range, closed[0], closed[0]))
        return results

    def add_ranges_from_string(
        self,
        ranges_string: str,
        days: dict = DAYS_EN,
        named_day_ranges: dict = NAMED_DAY_RANGES_EN,
        named_times: dict = NAMED_TIMES_EN,
        delimiters: list[str] = DELIMITERS_EN,
        closed: list[str] = CLOSED_EN,
    ) -> None:
        """
        Adds opening hour information from a localised string.
        :param ranges_string: localised string containing opening
                              time information.
        :param days: dictionary mapping localised day names to those
                     within DAYS ("Mo", "Tu", ...).
        :param named_day_ranges: dictionary mapping localised named
                                 day ranges to lists of days from
                                 DAYS ("Mo", "Tu", ...).
        :param named_times: dictionary mapping localised named times
                            of day to a tuple of equivalent 24hr time
                            and 12hr time respectively.
        :param delimiters: list of strings which are delimiters to
                           capture with the created regular
                           expression.
        :param closed: list of strings representing
                           localised meaning of "closed"
        """
        # Extract opening time information from localised string.
        results = OpeningHours.extract_hours_from_string(
            ranges_string=ranges_string,
            days=days,
            named_day_ranges=named_day_ranges,
            named_times=named_times,
            delimiters=delimiters,
            closed=closed,
        )

        # Add ranges to OpeningHours object.
        for result in results:
            for day in result[0]:
                self.add_range(day, result[1], result[2], closed=closed)
