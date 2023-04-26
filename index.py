from operator import and_
from turtle import pd

from mysql import connector


class MobileNumbers:
    def __init__(self, _chunk):
        self.chunk = _chunk

    # engine = create_engine("mysql+pymysql://root:password@some_mariadb/nepali_numbers?charset=utf8mb4")

    def seed_data(self):
        global i
        mydb = connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="numtest"
        )

        mycursor = mydb.cursor()

        mycursor.execute("SELECT id,operator,initial FROM mobile_number_types")

        myresult = mycursor.fetchall()

        for x in myresult:
            cpy = list(x)

            for i in range(self._get_million_chunk()):
                insert_list = list([])
                for j in range(self.chunk):
                    cpy[2] = int(str(x[2]) + "{:07d}".format(j+i*self.chunk))
                    t = tuple(cpy)
                    insert_list.append(t)
                sql = "INSERT INTO mobile_numbers (mobile_number_type_id, operator_type,mobile_number) VALUES (%s, %s,%s) ON DUPLICATE KEY UPDATE mobile_number  = VALUES(mobile_number)"
                print(i, x[2])
                print(insert_list[0])
                mycursor.executemany(sql, insert_list)
                mydb.commit()
                # break

            # print(insert_list)





    def _get_million_chunk(self, _mil=10000000):
        return int(_mil / self.chunk);


p1 = MobileNumbers(100000)
print(p1.seed_data())
