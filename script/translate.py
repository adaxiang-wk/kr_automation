import re
import json 

origin = "발행기관코드,발행기관명,표준코드,단축코드,채권분류,변경전 발행기관코드,상장여부,활성여부,상장일,상장폐지일,한글종목명,영문종목명,채권유형,지방채구분,발행금액,발행일,발행통화,상환일,특이채권유형,보증담보,보증률(%),이자지급방법,발행방법,발행구분,크라우드펀딩여부,표면이자율 확정여부,표면이자율(%),낙찰금리(%),발행가액(%),만기상환율(%),특이발행조건,옵션,선/후/중순위,상환방법,물가연동구분,참조지수(%),신종자본증권여부,조건부자본증권 유형,유동화,휴무관련(원금),이자지급기준,경과금리(%),휴무관련(이자),이자 선/후급,이자지급주기(개월),이자지급기준일구분,이자계산주기(개월),이자원단위 미만처리,이자월말구분,단수일이자기준,최초이자지급일,매출형태,선매출이자 지급방법,선/후매출일,기명구분,보장수익률(%),보장수익률적용일,추가수익률(%),추가수익률적용일,(전자)등록기관,원리금지급대행기관,대표주관회사,보증기관,신용평가기관1,신용등급,신용평가기관2,신용평가기관3,신용평가기관4,Call 주체,Call 사유"
translated = "Issuing agency code, issuing agency name, standard code, shortened code, bond classification, pre-change issuing agency code, listing status, active status, listing date, delisting date, Korean title, English title, bond type, local bond classification, issue amount, Issuance date, issuance currency, redemption date, special bond type, guarantee collateral, guarantee rate (%), interest payment method, issuance method, issuance category, whether or not crowdfunding, whether surface interest rate is determined, surface interest rate (%), successful bid rate (% ), issue price (%), maturity redemption rate (%), special issuance conditions, options, pre/post/intermediate priority, redemption method, price linkage classification, reference index (%), new capital stock, conditional capital stock type, Securitization, holiday-related (principal), interest payment criteria, accrued interest rate (%), holiday-related (interest), interest pre/post-payment, interest payment cycle (months), interest payment base date, interest calculation cycle (months), this resource unit Less than treatment, interest month end, single-day interest basis, initial interest payment date, sales type, pre-sales interest payment method, pre-/post-sales date, designated classification, guaranteed rate of return (%), guaranteed rate of return application date, additional rate of return (%), Additional rate of return application date, (electronic) registration agency, principal and interest payment agency, representative company, guarantee agency, credit rating agency 1, credit rating, credit rating agency 2, credit rating agency 3, credit rating agency 4, Call subject, Call reason"

ori_ls = origin.split(',')
tra_ls = translated.lower().replace(" ", "_")
tra_ls = re.sub(r"[\(\)]", "", tra_ls).split(",_")

trans_dict = {ori:tra for ori, tra in zip(ori_ls, tra_ls)}
print(trans_dict)

with open("kr_eng_translation.json", "w", encoding='UTF-8-sig') as outfile:  
    json.dump(trans_dict, outfile, ensure_ascii=False) 

