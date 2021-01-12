# Disclaimer: in case of any mistake identified in this script, 
# the reference percentages are the ones in the course outline
# at https://users.encs.concordia.ca/~tglatard/teaching/big-data/

def total(la1, la1b,
          la2,
          la3,
          la4, la4b, 
          proj_registration, proj_repository,
          proj_proposal, proj_presentation, proj_report,
          midterm, final=None):
    lab_total = (la1+la1b+la2+la3+la4+la4b)/4

    if proj_report == None: # current total, report grade is unknown
        proj_total = (proj_registration+proj_repository+proj_proposal*3+proj_presentation*10)/15
    else:
        proj_total = (proj_registration+proj_repository+proj_proposal*3+proj_presentation*10+proj_report*15)/30

    if final == None: # final is not taken (re-distribution)
        exam_total = midterm
    else:
        if final > midterm: # final compensates for midterm
            exam_total = final
        else:
            exam_total = (0.1*midterm + 0.3*final)/0.4
    
    total = lab_total*0.3 + proj_total*0.3 + exam_total*0.4
    return total


print(total(la1=100, la1b=10,
            la2=100,
            la3=100,
            la4=100, la4b=10,
            proj_registration=100, proj_repository=100,
            proj_proposal=73, proj_presentation=100, proj_report=None,
            midterm=94, final=None))