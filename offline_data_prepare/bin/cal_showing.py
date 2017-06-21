import sys
sys.path.append("conf")
import conf

def cal_showing():
    file_obj = open(conf.base_hot, 'r')
    showing_dict = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        if house_code.startswith("BJ"):
            house_code = "1010" + house_code[-8:]
        showing = tmp[1]
        pt = tmp[2]
        if house_code not in showing_dict:
            showing_dict[house_code] = {}
            showing_dict[house_code]["showing"] = 0
            showing_dict[house_code]["pt"] = pt
        showing_dict[house_code]["showing"] += int(showing)
        if pt > showing_dict[house_code]["pt"]:
            showing_dict[house_code]["pt"] = pt

    output_obj = open("data/showing_base", 'w')
    for house in showing_dict:
        str_line = house + "\t" + str(showing_dict[house]["showing"]) \
                   + "\t" + showing_dict[house]["pt"] + "\n"
        output_obj.write(str_line)

if __name__ == "__main__":
    cal_showing()
