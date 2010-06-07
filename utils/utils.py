
def str_cannonize(str):
   """ Input string with spaces and/or mixed upper and lower 
       characters will be converted to a cannonical form, 
       i.e. all to lowercase and spaces replaced by a '_'.

       Return  new cannonical string
   """
   if not str:
    return None

   tmp = str.split()  # intention is to replace multiple whitespaces by a single '_'
   new_str = ""
   for i in range(0,len(tmp) -1):
        new_str += tmp[i] + "_"

   new_str += tmp[-1]
   return new_str

