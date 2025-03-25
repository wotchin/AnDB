SELECT 
    count(author)
FROM TABULAR(
    SCHEMAS('{the most popular author} of the paper' as author text, '{top 5 keywords} of the paper' as keywords text, '{the key innovation} of the paper' as innovation text) 
    FROM File('nips_2024.txt')
    )
GROUP BY author;


SELECT 
    PROMPT('what is the title of the paper'),
    PROMPT('what is the main idea of the paper')
FROM File('nips_2024.txt')
WHERE EXTRACT('the {leader} of the paper') = 'Yoshua Bengio';


SELECT 
    PROMPT('what is the trend of the hot topics in the field of AI')
FROM File('nips_2024.txt') AS nips_2024 JOIN File('nips_2023.txt') AS nips_2023 
ON EXTRACT(nips_2024, 'the {year} of the paper') = EXTRACT(nips_2023, 'the {year} of the paper');



SELECT PROMPT('what is the popular trend in the field of AI') FROM File('nips_2024.txt'), File('nips_2023.txt'), File('nips_2022.txt');



