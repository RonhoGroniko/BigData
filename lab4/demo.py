import subprocess
import sys

PYTHON = sys.executable


def run_script(script_name: str) -> None:

    try:
        print(f"\n>>> Запуск {script_name}...\n")
        subprocess.call([PYTHON, script_name])
        print(f"\n<<< Завершён {script_name}\n")
    except FileNotFoundError:
        print(f"Ошибка: не найден скрипт {script_name}.")


def main_menu() -> None:

    while True:
        print("\n=======================================")
        print("   Демонстрация мини-поисковика")
        print("=======================================")
        print("1. Парсинг и сохранение в БД")
        print("2. Посчитать PageRank (MapReduce)")
        print("3. Посчитать PageRank (Pregel)")
        print("4. Поиск (term-at-a-time)")
        print("5. Поиск (document-at-a-time)")
        print("0. Выход")
        print("=======================================")
        choice = input("Выберите пункт меню: ").strip()

        if choice == "1":
            print("\n[Шаг 1] Парсинг (parser.py)")
            run_script("parser.py")
        elif choice == "2":
            print("\n[Шаг 2] PageRank через MapReduce (pagerank.py)")
            run_script("pagerank.py")
        elif choice == "3":
            print("\n[Шаг 3] PageRank через Pregel (pagerank_pregel.py)")
            run_script("pagerank_pregel.py")
        elif choice == "4":
            print("\n[Шаг 4] Полнотекстовый поиск (term-at-a-time)")
            run_script("term_at_a_time.py")
        elif choice == "5":
            print("\n[Шаг 5] Полнотекстовый поиск (document-at-a-time)")
            run_script("doc_at_a_time.py")
        elif choice == "0":
            print("Выход из демо.")
            break
        else:
            print("Неизвестный пункт, попробуйте ещё раз.")


if __name__ == "__main__":
    main_menu()
