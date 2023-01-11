if __name__ == '__main__':
    ### define logger
    lg.basicConfig(filename='round2sec.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logger = lg.getLogger('round2sec')
    logger.setLevel(lg.INFO)
    
    stdout_handler = lg.StreamHandler(sys.stdout)
    stdout_handler.setLevel(lg.INFO)
    logger.addHandler(stdout_handler)
    
    # ### define arguments 
    parser = argparse.ArgumentParser(prog = 'Round2Sec', description = 'Round to a resolution of seconds using mean value from data')
    parser.add_argument('-i', '--input') ### input json file
    args = json.load(open(parser.parse_args().input))
    