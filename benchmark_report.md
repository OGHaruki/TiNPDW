#  Raport

## 1. Cel projektu
Celem projektu było porównanie wydajności trzech różnych podejść do przetwarzania dużych zbiorów danych (plik CSV o rozmiarze ok. 3 GB, zawierający dane o przestępstwach w Nowym Jorku):
1. **PySpark RDD**: Tradycyjny interfejs Sparka oparty na rozproszonych kolekcjach obiektów.
2. **PySpark SQL**: Nowoczesny, zoptymalizowany silnik Sparka z optymalizatorem Catalyst.
3. **SQLite**: Tradycyjna, relacyjna baza danych SQL uruchomiona w środowisku WSL.

## 2. Środowisko testowe
* **System operacyjny:** Windows 11 + WSL2 (Ubuntu)
* **Python:** 3.12.9
* **Spark:** 3.5.0
* **JDK:** OpenJDK 21.0.10

## 3. Zestawienie wyników (Czas wykonania w sekundach)

| Zapytanie | PySpark RDD | PySpark SQL | SQLite |
| :--- | :---: | :---: | :---: |
| **Import danych** | 6.55s | 28.83s | 224.52s |
| **Q1:** Top 10 najczęstszych zgłoszeń | 31.23s | 15.66s | 0.72s |
| **Q2:** Top 3 najczęstsze przestępstwa w każdej dzielnicy | 158.8s | 87.38s | 8.57s |
| **Q3:** Top 3 urzędy (i ich top 3 skargi) | 101.8s | 55.58s | 7.54s |
| **Q4:** Top 4 lokalizacje (i ich top 3 skargi) | 129.26s | 68.44s | 29.95s |
| **Q5:** Procentowy udział grup wiekowych | 28.18s | 13.13s | 4.8s |

## 4. Wyniki zapytań
### Q1: Top 10 najczęstszych zgłoszeń

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 1666745 |
| 2 | HARRASSMENT 2 | 1272980 |
| 3 | ASSAULT 3 & RELATED OFFENSES | 998322 |
| 4 | CRIMINAL MISCHIEF & RELATED OF | 916268 |
| 5 | GRAND LARCENY | 831964 |
| 6 | DANGEROUS DRUGS | 471830 |
| 7 | OFF. AGNST PUB ORD SENSBLTY & | 455183 |
| 8 | FELONY ASSAULT | 393369 |
| 9 | ROBBERY | 331515 |
| 10 | BURGLARY | 310292 |

### Q2: Top 3 najczęstsze przestępstwa w każdej dzielnicy

#### BRONX

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 281854 |
| 2 | PETIT LARCENY | 281235 |
| 3 | ASSAULT 3 & RELATED OFFENSES | 249810 |

#### QUEENS

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 336803 |
| 2 | HARRASSMENT 2 | 270699 |
| 3 | CRIMINAL MISCHIEF & RELATED OF | 211190 |

#### MANHATTAN

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 523016 |
| 2 | GRAND LARCENY | 312754 |
| 3 | HARRASSMENT 2 | 255295 |

#### BROOKLYN

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 453861 |
| 2 | HARRASSMENT 2 | 380140 |
| 3 | ASSAULT 3 & RELATED OFFENSES | 301087 |

#### STATEN ISLAND

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 84381 |
| 2 | PETIT LARCENY | 67656 |
| 3 | CRIMINAL MISCHIEF & RELATED OF | 56801 |

### Q3: Top 3 urzędy (i ich top 3 skargi)

#### N.Y. POLICE DEPT

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 1595343 |
| 2 | HARRASSMENT 2 | 1128620 |
| 3 | ASSAULT 3 & RELATED OFFENSES | 868949 |

#### N.Y. HOUSING POLICE

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 124950 |
| 2 | ASSAULT 3 & RELATED OFFENSES | 100417 |
| 3 | DANGEROUS DRUGS | 78186 |

#### N.Y. TRANSIT POLICE

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | GRAND LARCENY | 26541 |
| 2 | CRIMINAL MISCHIEF & RELATED OF | 24574 |
| 3 | ASSAULT 3 & RELATED OFFENSES | 20532 |

### Q4: Top 4 lokalizacje (i ich top 3 skargi)

#### STREET

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | PETIT LARCENY | 436837 |
| 2 | CRIMINAL MISCHIEF & RELATED OF | 380458 |
| 3 | DANGEROUS DRUGS | 280424 |

#### RESIDENCE - APT. HOUSE

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 432687 |
| 2 | ASSAULT 3 & RELATED OFFENSES | 299544 |
| 3 | OFF. AGNST PUB ORD SENSBLTY & | 197749 |

#### RESIDENCE-HOUSE

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 196184 |
| 2 | ASSAULT 3 & RELATED OFFENSES | 108307 |
| 3 | OFF. AGNST PUB ORD SENSBLTY & | 97755 |

#### RESIDENCE - PUBLIC HOUSING

| # | Kategoria | Liczba |
| :---: | :--- | ---: |
| 1 | HARRASSMENT 2 | 124410 |
| 2 | ASSAULT 3 & RELATED OFFENSES | 99835 |
| 3 | DANGEROUS DRUGS | 76737 |

### Q5: Procentowy udział grup wiekowych

| Grupa wiekowa | Udział |
| :--- | ---: |
| 25-44 | 40.25% |
| 45-64 | 20.84% |
| UNKNOWN | 16.73% |
| 18-24 | 12.06% |
| <18 | 5.59% |
| 65+ | 4.53% |

*W tabeli uwzględniono tylko grupy wiekowe stanowiące więcej niż 0%. Zbiór danych wejściowych został najpierw przefiltrowany, aby usunąć wartości puste oraz null.*

![Udział grup wiekowych ofiar (Spark RDD)](output/victims_by_age_group_spark_rdd.png)


## 5. Wnioski i obserwacje
### 5.1 Porównanie podejść (RDD vs Spark SQL vs SQLite)
- **Spark SQL vs RDD:** we wszystkich zapytaniach **Spark SQL był wyraźnie szybszy** od podejścia RDD.

- **SQLite vs Spark:** dla zapytań analitycznych **SQLite okazało się najszybsze** w każdym z zapytań.

- **Czas przygotowania danych:** przygotowanie danych w tradycyjnej bazie danych jest wielokrotnie dłuższe niż w Sparku.

*W celu sprawiedliwego porównania dane nie były cachowane.*

### 5.2 Charakter zapytań a czasy
Najbardziej „kosztowne” były zapytania typu **Top N w wielu grupach** (Q2–Q4). Wynika to z faktu, że takie zapytania wymagają agregacji po wielu kluczach oraz wyboru Top N osobno dla każdej grupy, co w Sparku zwykle wiąże się z kosztownym przetasowaniem danych (shuffle). Szczególnie:

1. **Q2** – to zapytanie polega na policzeniu liczności dla par *(BORO_NM, OFNS_DESC)* i wybranie top 3 kategorii osobno dla każdej dzielnicy. To duża agregacja „w poprzek” całego zbioru (wiele kombinacji kluczy), po której dochodzi etap wyboru top N w ramach każdej dzielnicy. W Sparku taki wzorzec generuje wiele przetasowań, a w podejściu RDD dodatkowy narzut związany z serializacją.
2. **Q3 i Q4** – są dwuetapowe: najpierw wyznaczane są najczęstsze agencje (Q3) lub lokalizacje (Q4), a dopiero potem liczone jest top 3 kategorii w każdej z tych grup. W porównaniu do Q2 takie podejście bywa szybsze, bo drugi etap działa na mniejszej liczbie grup (np. 3 agencje / 4 lokalizacje) i zwykle na istotnie zawężonym podzbiorze danych po filtrze. W efekcie jest mniej pracy agregacyjnej i mniej danych do przetasowania, co przekłada się na niższy czas wykonania niż w Q2.
3. **Q1 i Q5** – to proste agregacje globalne, które nie wymagają przetasowania danych, więc są najszybsze w obu środowiskach.

### 5.4 Podsumowanie
Na podstawie uzyskanych wyników najlepsze czasy pojedynczych zapytań (Q1–Q5) osiągnęło SQLite, jednak kosztem bardzo długiego etapu przygotowania danych (import i indeksowanie). W przypadku pracy na już załadowanej bazie i wykonywania wielu powtarzalnych zapytań jest to podejście najbardziej opłacalne. Z kolei Spark charakteryzował się znacznie szybszym czase przygotowania danych, ale dłuższym czasem wykonania zapytań, szczególnie tych bardziej złożonych. \
Ponadto Spark SQL był zdecydowanie szybszy od podejścia RDD, co wynika z faktu, że Spark SQL korzysta z optymalizatora zapytań Catalyst, który potrafi efektywnie planować i optymalizować wykonanie zapytań, podczas gdy RDD wymaga ręcznego zarządzania operacjami transformacji i akcji, co często prowadzi do mniej efektywnego wykorzystania zasobów.\
Wybór konkretnej technologii powinien być uzależniony od charakteru zadania, wymagań dotyczących czasu odpowiedzi oraz dostępnych zasobów. Dla jednorazowych analiz na dużych zbiorach danych Spark może być bardziej efektywny, natomiast dla powtarzalnych zapytań na już przygotowanej bazie danych SQLite będzie lepszym wyborem.

