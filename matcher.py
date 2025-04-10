import psycopg2
from psycopg2 import sql
from typing import List, Dict, Tuple


class VacancyMatcher:
    def __init__(self, db_connection_params: Dict):
        self.conn_params = db_connection_params

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def normalize_skills(self, skills_str: str) -> List[str]:
        """Нормализует строку навыков: разбивает по запятым, убирает пробелы, приводит к нижнему регистру"""
        if not skills_str:
            return []
        return [skill.strip().lower() for skill in skills_str.split(',')]

    def calculate_match_score(self, student_skills: List[str], vacancy_skills: List[str]) -> float:
        """
        Вычисляет оценку соответствия между навыками студента и вакансии.
        Возвращает значение от 0 до 1, где 1 - полное соответствие.
        """
        if not vacancy_skills:
            return 0.0

        student_skills_set = set(student_skills)
        vacancy_skills_set = set(vacancy_skills)

        # Количество совпадающих навыков
        common_skills = student_skills_set & vacancy_skills_set
        match_score = len(common_skills) / len(vacancy_skills_set)

        return match_score

    def find_matching_vacancies(self, name: str) -> List[Tuple]:
        """
        Находит вакансии, наиболее подходящие студенту по навыкам.
        Возвращает список вакансий, отсортированных по степени соответствия.
        """
        query = sql.SQL("""
            WITH student_skills AS (
                SELECT skills FROM si.students WHERE name = %s
            )
            SELECT 
                v.vacancy_id,
                v.external_id,
                v.title,
                v.area_name,
                v.company_name,
                v.required_skills,
                v.vacancy_link,
                s.skills AS student_skills
            FROM si.vacancies v, student_skills s
            WHERE v.required_skills IS NOT NULL AND v.required_skills != ''
            ORDER BY v.created_at DESC
        """)

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (name,))
                    vacancies = cursor.fetchall()

                    if not vacancies:
                        return []

                    # Получаем навыки студента из первой строки (они одинаковы для всех вакансий)
                    student_skills_str = vacancies[0][7]  # student_skills находится в 8-й позиции (индекс 7)
                    student_skills = self.normalize_skills(student_skills_str)

                    # Оцениваем каждую вакансию
                    scored_vacancies = []
                    for vac in vacancies:
                        vacancy_skills = self.normalize_skills(
                            vac[5])  # required_skills находится в 6-й позиции (индекс 5)
                        match_score = self.calculate_match_score(student_skills, vacancy_skills)

                        if match_score > 0:
                            scored_vacancies.append((*vac[:7], match_score))  # Исключаем student_skills из результата

                    # Сортируем по убыванию оценки соответствия
                    scored_vacancies.sort(key=lambda x: x[7], reverse=True)

                    return scored_vacancies

        except Exception as e:
            print(f"Ошибка при поиске вакансий: {e}")
            return []

    def recommend_vacancies(self, name: str) -> List[Dict]:
        """Рекомендует вакансии студенту в удобном формате"""
        vacancies = self.find_matching_vacancies(name)

        recommendations = []
        for vac in vacancies:
            recommendations.append({
                'vacancy_id': vac[0],
                'external_id': vac[1],
                'title': vac[2],
                'location': vac[3],
                'company': vac[4],
                'required_skills': vac[5],
                'link': vac[6],
                'match_score': f"{vac[7] * 100:.1f}%"
            })

        return recommendations


