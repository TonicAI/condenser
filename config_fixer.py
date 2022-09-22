def config_fixer(target,db_name,is_tablename=False):

  if isinstance(target,str) and is_tablename:
    if (db_name + '.') not in target:
      return db_name + '.' + target
    return target
    
  elif isinstance(target,list):
    return [config_fixer(list_target,db_name,is_tablename) for list_target in target]

  elif isinstance(target,dict):

    new_dict = {}
    for mapkey in target:
      new_dict[mapkey] = config_fixer(target[mapkey],db_name,("table" in mapkey))

    return new_dict

  else:
    return target