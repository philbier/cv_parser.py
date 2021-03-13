import re
import sys
import textract
import spacy
import pandas as pd

class ConsultantProfile: 
    def __init__(self, path, file):
        self.path = path
        self.file = file
        #self.text = textract.process(self.path+"\\"+self.file).decode('utf-8')
        self.text = textract.process(self.file).decode('utf-8')
        self.section_boundries = {
                    'industry_kh' : {'start' : 'Industry Know-how', 'end' : 'Professional Career'},
                    'func_and_meth' : {'start' : 'Functional and Method Competencies', 'end' : 'IT Competencies'},
                    'it_comp' : {'start' : 'IT Competencies', 'end' : 'Certifications'},
                    'projects' : {'start' : 'Project Experience', 'end' : None}
        }
        self.industrySection = self.__clean_industrySection( self.__get_section('industry_kh') )
        self.functAndMethSection = self.__clean_functAndMethSection( self.__get_section('func_and_meth') )
        self.itSection = self.__clean_itSection( self.__get_section('it_comp') )
        self.projectSection = self.__clean_projectSection( self.__get_section('projects') )
        self.spacy_model = spacy.load('en_core_web_sm') #use small space model

    def __get_section(self, sect_key):
        try:
            start, end = self.section_boundries[sect_key].values()
            t = self.text.split(start)[1] 
            section_text = t.split(end)[0] if sect_key != 'projects' else t
            return section_text
        except Exception as e:
            print(rf"Fehler bei 'get_section' von '{sect_key['start']}' fuer '{self.file}'")
            print(rf"Fehler: {e}")

    def __clean_industrySection(self, section_text):
        try:
            sect = section_text.split('\n\n')
            return [x for x in sect if x]
        except Exception as e:
            print(rf"Fehler in Funktion '{sys._getframe(  ).f_code.co_name}' fuer '{self.file}'")
            print(rf"Fehler: {e}")

    def __clean_functAndMethSection(self, section_text):
        try:
            sect = section_text.replace('\t', '').split('\n\n')
            return [x for x in sect if x]
        except Exception as e:
            print(rf"Fehler in Funktion '{sys._getframe(  ).f_code.co_name}' fuer '{self.file}'")
            print(rf"Fehler: {e}")

    def __clean_itSection(self, section_text):
        try:
            sect = section_text.replace('\t', '').split('\n\n')
            return [x for x in sect if x]
        except Exception as e:
            print(rf"Fehler in Funktion '{sys._getframe(  ).f_code.co_name}' fuer '{self.file}'")
            print(rf"Fehler: {e}")

    def __clean_projectSection(self, section_text):
        try:
            sect = section_text.split('\n\n\n\n')
            sect = list(map(lambda x : x.replace('\n\n', '. ').replace('\t', ' '), sect))
            # regex .+ will match everything followed by the website footer
            regex = re.compile(r'.+(www.q-perior.com)')
            return [x for x in sect if not regex.match(x) and x != ' ' and x != '']
        except Exception as e:
            print(rf"Fehler in Funktion '{sys._getframe(  ).f_code.co_name}' fuer '{self.file}'")
            print(rf"Fehler: {e}")
            
    def get_noun_chunks(self, phrase_list):
        try:
            doc = self.spacy_model(phrase_list)
            noun_chunks = []
            for i, sentence in enumerate(doc.sents):
                for noun in sentence.noun_chunks:
                    noun_chunks.append(noun)
            return noun_chunks
        except Exception as e:
            print(rf"Fehler in {sys._getframe(  ).f_code.co_name}")
            print(rf"Fehler: {e}")

    def remove_stopwords(self, phrase_list):
        try:
            lst = []
            for phrase in phrase_list:
                doc = self.spacy_model(str(phrase))
                token_list = [token.text for token in doc if not token.is_stop]
                token_list = (" ").join(token_list)
                lst.append(token_list)
            return lst
        except Exception as e:
            print(rf"Fehler in {sys._getframe(  ).f_code.co_name}")
            print(rf"Fehler: {e}")
    
    def __has_entity_type(self, text):
        try:
            doc = self.spacy_model(text)
            bol = False
            for entity in doc.ents:
                if entity.label_ == 'DATE':
                    bol = True
                    break
            return bol
        except Exception as e:
            print(rf"Fehler in {sys._getframe(  ).f_code.co_name}")
            print(rf"Fehler: {e}")
    
    def remove_named_entities(self, phrase_list):
        try:
            return [x for x in phrase_list if not self.__has_entity_type(x)]
        except Exception as e:
            print(rf"Fehler in {sys._getframe(  ).f_code.co_name}")
            print(rf"Fehler: {e}")
    
class SkillList:
    def __init__(self, path, file):
        self.path = path
        self.file = file
        self.list = self.__open_file()
    
    # def __open_file(self):
    #     with open(self.path+"\\"+self.file, encoding="utf-8") as f:
    #         text = f.read().splitlines()
    #     return text

    def __open_file(self):
        with open(self.file, encoding="utf-8") as f:
            text = f.read().splitlines()
        return text 

    def get_list_matching_skills(self, phrase_list):
        lst = []
        for phrase in phrase_list:
            for skill in self.list:
                skill_adj = skill.replace(' ', '_')
                term = re.escape(rf'{(skill_adj)}')
                pattern = re.compile(r'\b' + term + r'\b' , re.IGNORECASE)
                match_obj = re.search(pattern, phrase)
                if match_obj != None:
                    lst.append(phrase)    
        return list(dict.fromkeys(lst))    

class Pipeline:  
    def process_skill_section(self, cp_obj, skl_lst_obj, phrase_text):
        tmp_lst = cp_obj.get_noun_chunks(phrase_text)
        tmp_lst = cp_obj.remove_stopwords(tmp_lst)
        tmp_lst = cp_obj.remove_named_entities(tmp_lst)
        tmp_lst = skl_lst_obj.get_list_matching_skills(tmp_lst)
        return tmp_lst

    def create_empty_dataframe(self):
        return pd.DataFrame([], columns=['consultant_name','skill_type', 'detail', 'skill'])
    
    def get_dataframe(self, lst, skill_type, detail, consultant_name):
        df = pd.DataFrame(lst, columns=['skill'])
        df['skill_type'] = skill_type
        df['detail'] = detail
        df['consultant_name'] = consultant_name
        df = df[['consultant_name','skill_type', 'detail', 'skill']]
        return df
 
if __name__ == '__main__':
    path = r""
    file = r'CV_Phillip_Biermann_en_Mar20.docx'
    skill_file = r'skills_short.txt'
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.expand_frame_repr', False)

    ### Instantiation
    cp = ConsultantProfile(path, file)
    skill_list = SkillList(path, skill_file)
    pipe = Pipeline()
    union_df = pipe.create_empty_dataframe()

    ### building pipeline
    df_ind = pipe.get_dataframe(cp.industrySection, 'Industry', None, 'Phillip_Biermann')
    union_df = pd.concat([union_df,df_ind])

    df_it = pipe.get_dataframe(cp.itSection, 'IT', None, 'Phillip_Biermann')
    union_df = pd.concat([union_df,df_it])

    for index, line in enumerate(cp.functAndMethSection):
        func_skills = pipe.process_skill_section(cp, skill_list, line)
        df_func = pipe.get_dataframe(func_skills, 'Functional & Methodical', None, 'Phillip_Biermann')
        union_df = pd.concat([union_df,df_func])

    proj_List_length = len(cp.projectSection)
    for index, project in enumerate(cp.projectSection):
        proj_skills = pipe.process_skill_section(cp, skill_list, project)
        df_proj = pipe.get_dataframe(proj_skills, 'Project Experience', rf'Project {proj_List_length}', 'Phillip_Biermann')
        union_df = pd.concat([union_df,df_proj])
        proj_List_length -= 1
    print(union_df)



    
   
    
   
    
    