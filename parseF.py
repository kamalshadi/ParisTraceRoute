import re
import os
import sqlite3 as sq
from datetime import datetime
from calendar import timegm
try:
	import SubnetTree
except ImportError:
	print 'Error:Please install SubnetTree package...'
	
def bgp(bgpFile):
	i=0
	bf=open('BGP/'+bgpFile,'r')
	t = SubnetTree.SubnetTree()
	print 'Building subnet network from BGP table'
	asDic={}
	ASdic={}
	for lines in bf:
		if i==0:
			i=1
			continue
		net=lines.strip().split()[-1].strip()
		asn=lines.strip().split()[0].strip()
		asDic[net]=asn
		t[net]=net
	return (t,asDic)
	

def parseF(dirr,fName,t,AS):
	ip1='[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}:'
	ip2='\([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\)'
	lt='^\s*[0-9]+\s+'
	checkLine=re.compile(lt)
	ippat1=re.compile(ip1)
	ippat2=re.compile(ip2)
	i=-1
	with open(dirr+'/'+fName,'r') as f:
		R=[]
		H=[]
		for line in f:
			if i==-1:
				i=1
				res=ippat1.findall(line)
				try:
					ipa='"'+res[1].strip(': \n')+'"'
					ser='"'+res[0].strip(': \n')+'"'
					try:
						netC=t[ipa.strip('"')]
						asnC='"'+AS[netC]+'"'
					except KeyError:
						asnC='"?"'
					try:
						netS=t[ser.strip('"')]
						asnS='"'+AS[netS]+'"'
					except KeyError:
						asnS='"?"'
				except IndexError:
					return None
			else:
				nu=checkLine.search(line)
				if nu:
					try:
						res=ippat2.findall(line)[0].strip('() \n')
						try:
							net=t[res].strip(' \n"')
							asn=AS[net]
						except KeyError:
							asn='?'
						H.append(asn)
						R.append(res)
					except IndexError:
						R.append('?')
						H.append('?')
						continue
	return (ipa,ser,asnC,asnS,'"'+','.join(R)+'"','"'+','.join(H)+'"')
	
	
	
	
if __name__=='__main__':
	dir='201401'
	bgpFile='01jan14'
	for s,d,f in os.walk(dir):
		break
	if os.path.exists(dir+'/paris.db'):
		print 'Error:DB already exists in '+dir
	else:
		t,AS=bgp(bgpFile)
		tp=re.compile('\d{8}T\d{2}:\d{2}:\d{2}')
		D=sq.connect(dir+'.db')
		c=D.cursor()
		c.execute("""Create table meta(ip text not null,time text not null,
		ClientAS text not null,ServerAS text not null,hops text not null,
		AS_hops text not null,server text not null)""")
		ll=len(f)
		for k,fName in enumerate(f):
			if k%500==0:
				print 'Percentage: '+str(round(float(k)*10000/ll)/100)
			try:
				tt=tp.findall(fName)[0]
			except IndexError:
				continue
			ymd,hms=tt.split('T')
			hh,mm,ss=hms.split(':')
			do=datetime(int(ymd[0:4]),int(ymd[4:6]),int(ymd[6:8]),int(hh),int(mm),int(ss))
			tt='"'+str(timegm(do.utctimetuple()))+'"'
			try:
				ipa,ser,asnC,asnS,h1,h2=parseF(dir,fName,t,AS)
			except TypeError:
				continue
			qq='insert into meta values('+ipa+','+tt+','+asnC+','+asnS+','+h1+','+h2+','+ser+')'
			c.execute(qq)
		D.commit()
		D.close()
			
