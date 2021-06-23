#! /usr/bin/env python3
import os, subprocess, glob, time
from multiprocessing.pool import ThreadPool
os.chdir(os.path.dirname(__file__))


start = time.time()
maxTime = 3600*3        # Keep the time this script is running limited
eras = ['Summer20UL16pre', 'Summer20UL16post', 'Summer20UL17', 'Summer20UL18']
# eras = ['Summer20UL17']

def system(command):
  try:
    return subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT).decode()
  except subprocess.CalledProcessError as e:
    print(e.output)
    return 'error'

# The release does not matter too much as long as it can read the miniAOD files, but might need to be updated in the future when moving to new production eras
def setupCMSSW():
  arch='slc7_amd64_gcc700'
  release='CMSSW_11_3_0'
  # arch='slc6_amd64_gcc700'
  # release='CMSSW_10_2_20'
  setupCommand  = 'export SCRAM_ARCH=' + arch + ';'
  setupCommand += 'source /cvmfs/cms.cern.ch/cmsset_default.sh;'
  if not os.path.exists(release):
    setupCommand += '/cvmfs/cms.cern.ch/common/scram project CMSSW ' + release + ';'
  setupCommand += 'cd ' + release + '/src;'
  setupCommand += 'eval `/cvmfs/cms.cern.ch/common/scram runtime -sh`;'
  setupCommand += 'cd ../../'
  return setupCommand

# See what is already available and load it, to avoid this script runs forever to do things which are already done
def loadExisting(filename):
  try: 
    with open(filename) as f: return {l.split()[0] : l for l in f if 'pnfs' in l}
  except:
    return {}

# system('wget https://raw.githubusercontent.com/GhentAnalysis/privateMonteCarloProducer/master/monitoring/crossSectionsAndEvents.txt -O crossSectionsAndEventsOnGit.txt')
# system('wget https://raw.githubusercontent.com/GhentAnalysis/privateMonteCarloProducer/master/monitoring/eventCounters.txt.xz -O eventCountersOnGit.txt.xz')
currentLinesGit = loadExisting('crossSectionsAndEventsOnGit.txt')
currentLines    = loadExisting('crossSectionsAndEvents.txt')


def getExistingLine(directory):
  existingLine = None
  if directory in currentLines and 'files' in currentLines[directory]:
    existingLine = currentLines[directory]
  if directory in currentLinesGit and 'files' in currentLinesGit[directory]:
    if not existingLine or int(currentLinesGit[directory].split('files')[0].split()[-1]) > int(existingLine.split('files')[0].split()[-1]):
      return currentLinesGit[directory]
  return existingLine


# If the cross section is not known yet, calculate it
def getCrossSection(cms_das_name):
  # output = system('%s;cmsRun xsecAnalyzer.py inputDir=%s' % (setupCMSSW(), directory))
  in_files = getInputFiles(cms_das_name)
  print('%s;cmsRun $CMSSW_BASE/src/ana.py inputFiles="%s"' % (setupCMSSW(), in_files))
  output = system('%s;cmsRun $CMSSW_BASE/src/ana.py inputFiles="%s"' % (setupCMSSW(), in_files))
  for line in output.split('\n'):
    if 'After filter: final cross section = ' in line:
      return line.split('= ')[-1].rstrip()
  else:
    return -1

# Store the number of events per file
system('unxz eventCounters.txt.xz;unxz eventCountersOnGit.txt.xz')
eventCounters = loadExisting('eventCounters.txt')
eventCounters.update(loadExisting('eventCountersOnGit.txt'))
newEventCounters = {}
def eventsPerFile(filename):
  if filename in eventCounters:
    return eventCounters[filename].split()[-1]
  output = system('%s;edmFileUtil %s | grep events' % (setupCMSSW(), filename.replace('/pnfs/iihe/cms','')))
  try:
    events = int(str(output).split('events')[0].split()[-1])
    newEventCounters[filename] = '%-180s %8s\n' % (filename, events)
    return events
  except:
    return None

# If the number of files is updated, recalculate the number of events
def getEvents(directory):
  files = glob.glob(os.path.join(directory, '*.root'))
  existingLine = getExistingLine(directory)
  if existingLine:
    if existingLine.count('files')==1 and int(existingLine.split('files')[0].split()[-1])==len(files) and not '?' in existingLine:
      return '%s files' % len(files), '%s events' % existingLine.split()[-2]
  events = []
  for f in files:
    if (time.time() - start) > maxTime: events += [None]
    else:                               events += [eventsPerFile(f)]
  try:    events = sum([int(e) for e in events])
  except: events = '?'
  return '%s files' % len(files), '%s events' % events

def getLine(das_entry):
  return '%-170s %30s\n' % ((das_entry, getCrossSection(das_entry)))

def getInputFiles(das_entry):
  import subprocess, shlex
  command_line = '/cvmfs/cms.cern.ch/common/dasgoclient --query "file dataset={0}"'.format(das_entry)
  args = shlex.split(command_line)
  p = subprocess.Popen(args, stdout=subprocess.PIPE)
  (out, err) = p.communicate()
  out_files = [x for x in out.decode().split('\n') if x]
  return ",".join(out_files[:35])


# Rewrite the file and calculate the x-sec for the new ones
# samples = '*'
with open('crossSectionsAndEvents.txt',"w") as f:
  for era in eras:
    f.write('%s\n\n' % (era))
    cms_das_names = [line.split('%')[0].strip() for line in open('dataset_{}.txt'.format(era), 'r')]
    cms_das_names = [line for line in cms_das_names if line]
    pool = ThreadPool(processes=16)
    linesToWrite = pool.map(getLine, cms_das_names) 
    pool.close()
    pool.join()
    # linesToWrite = [getLine(cms_das_name) for cms_das_name in cms_das_names]
    for line in sorted(linesToWrite): f.write(line)
    f.write('\n')

eventCounters.update(newEventCounters)
with open('eventCounters.txt', 'w') as f:
  for line in sorted(eventCounters.values()):
    f.write(line)

# system('rm *OnGit.txt')
# system('xz -f eventCounters.txt')
# system('git add crossSectionsAndEvents.txt;git add eventCounters.txt.xz;git commit -m"Update of cross sections and events"') # make sure this are separate commits (the push you have to do yourself though)
