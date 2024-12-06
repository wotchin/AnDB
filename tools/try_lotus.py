import pandas as pd
import lotus
from lotus.models import LM

# configure the LM, and remember to export your API key
lm = LM(model="gpt-4o-mini")
lotus.settings.configure(lm=lm)

# create dataframes with course names and skills
courses_data = {
    "Course Name": [
        "History of the Atlantic World",
        "Riemannian Geometry",
        "Operating Systems",
        "Food Science",
        "Compilers",
        "Intro to computer science",
    ]
}
skills_data = {"Skill": ["Math", "Computer Science"]}
courses_df = pd.DataFrame(courses_data)
skills_df = pd.DataFrame(skills_data)

# lotus sem join 
res = courses_df.sem_join(skills_df, "Taking {Course Name} will help me learn {Skill}")
print(res)

# Print total LM usage
lm.print_total_usage()