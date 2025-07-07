import json
import time
import re
import asyncio
import warnings
from typing import Optional, Dict, List, Any, Union
from types import SimpleNamespace

import aiohttp
import requests
from bs4 import BeautifulSoup

from recruits import (_AGENTS, _TIMEOUT, _rand_headers,_get_user_stat,_parse_id)

'''
FalseDmitry: the class for collecting GoodReads user data.
'''

class FalseDmitry:
    '''Dmitry Ivanovich, Lazarus of the Motherland'''
    def __init__(self):
        '''GoodReads user data scraper. Sequential and asynchronous capabilities available.'''
        self.soup = None
        self.user_url = None
    
    async def load_user_async(self,
                              session: aiohttp.ClientSession,
                              user_identifier: str) -> None:
        '''loads Goodreads USER data asynchronously
        
        :param session:
         an aiohttp.ClientSession object
        :param user_identifier (str):
         url to GoodReads User page, or unique User identifier:
        '''
        try:
            if user_identifier:
                if len(re.compile(r'^https://www.goodreads.com/user/show/\d*').findall(user_identifier)) > 0:
                    user_identifier = user_identifier
                elif len(re.compile(r'^\d*$').findall(user_identifier)) > 0:
                    user_identifier = f'https://www.goodreads.com/user/show/{user_identifier}'
                else:
                    raise ValueError('user_identifier must be full URL string OR user identification number')
            else:
                raise ValueError('user_identifier must be full URL string OR user identification number')
            self.user_url = user_identifier
            
            u_id = _parse_id(self.user_url)
            print(f'ATTEMPT {u_id} @ {time.ctime()}')

            async with session.get(url=self.user_url,
                                   headers=_rand_headers(_AGENTS)) as resp:
                if resp.status != 200:
                    print(f'{resp.status} for {self.b_url}')
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')

                if soup.find('div', {'id':'privateProfile'}):
                    raise Exception(f'User {u_id} is private. Returning None')
                    
                info_main = soup.find('div',class_='mainContentFloat')
                info_left = info_main.find('div', class_='leftContainer')
                info_right = info_main.find('div', class_='rightContainer')

                self._soup = soup
                self._info_main = info_main
                self._info_left = info_left
                self._info_right = info_right
                
                print(f'PULLED {u_id} @ {time.ctime()}')
                return self
    
        except asyncio.TimeoutError:
            print(f'TIMEOUT ERROR for {u_id}')
            return None
        except aiohttp.ClientError as er:
            print(f'CLIENT ERROR for {u_id}: {er}')
            return None
        except Exception as er:
            print(f'OTHER ERROR for {u_id}: {er}')
            return None


    def get_name(self) -> Optional[str]:
        '''returns name of user'''
        name_box = self._info_left.find('h1', class_='userProfileName')
        if name_box:
            return name_box.text.strip()
        else:
            return None
    

    def get_id(self) -> Optional[str]:
        '''returns user ID.'''
        return _parse_id(self.user_url)
        

    def get_image_path(self) -> Optional[str]:
        '''returns image path of user'''
        pic_box = self._info_left.find('div',class_='leftAlignedProfilePicture')
        if pic_box:
            pfp = pic_box.find('img')
            if pfp:
                pfp_path = pfp['src']
                return pfp_path
        return None
    

    def get_num_ratings(self) -> Optional[float]:
        '''returns number of ratings given by user'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('ratings',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'num_ratings')
            return None
    

    def get_avg_ratings(self) -> Optional[float]:
        '''returns average of ratings given by user'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('avg',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'avg_ratings')
            return None
        

    def get_num_reviews(self) -> Optional[int]:
        '''returns number of reviews given by user'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('review',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'num_reviews')
            return None
    
    
    def get_favorite_genres(self) -> Optional[List[str]]:
        '''returns a user's favorite genres'''
        g_list = []
        genre_box = self._info_right.find_all('div',class_='stacked clearFloats bigBox')
        if genre_box:
            genre_box = genre_box[-1]
            genres_list = genre_box.find('div',class_='bigBoxContent containerWithHeaderContent')
            if genres_list:
                genres = genres_list.find_all('a')
                if genres:
                    for genre in genres:
                        g = genre.text.strip()
                        g_list.append(g)
                    return g_list
        return None


    def get_featured_shelf(self) -> Optional[Dict[str,List[Dict]]]: # a mess of an annotation, sorry
        '''returns a user's featured shelf'''
        fs_box = self._info_left.find('div', {'id':'featured_shelf'})
        if fs_box:
            fs_title = fs_box.find('h2').find('a').text.strip()
            img_grid = fs_box.find('div', class_='imgGrid')
        else:
            img_grid = None

        dat_dict = {}
        if img_grid:
            if len(img_grid.find_all('a')) > 0:
                dat = []
                for obj in img_grid.find_all('a'):
                    bk_url = re.sub(r'^.*show\/|\..*$','',obj['href'])
                    bk_title = obj.find('img')['title']
                    bk_dat = {
                        'id': bk_url,
                        'title': bk_title
                    }
                    dat.append(bk_dat)
                dat_dict[fs_title] = dat
                return dat_dict
        return None


    def get_follower_count(self) -> Optional[int]:
        '''returns user's follower count'''
        margin_links = self._info_right.find_all('a',class_='actionLinkLite')
        if len(margin_links) > 0:
            for lnk in margin_links:
                lnk_text = lnk.text
                if 'are following' in lnk_text:
                    follow_count = re.sub(r'\speople are.*$','',lnk_text)
                    follows = int(follow_count)
                    return follows
        return None


    def get_followings_sample(self) -> Optional[List[Dict[str,Any]]]:
        '''returns a sample of user's followings'''
        boxes = self._info_right.find_all('div',class_='clearFloats bigBox')
        for box in boxes:
            title = box.find('a').text
            if re.search(r'.*is Following',title):
                follow_box = box.find('div',class_='bigBoxContent containerWithHeaderContent')
                follow_dat = []
                for follower in follow_box.find_all('div'):
                    flwr = follower.find('a')
                    if flwr:
                        flwr_name = flwr['title']
                        flwr_id = flwr['href']
                        flwr_id = re.sub(r'^.*show\/|-.*$|\.*$','',flwr_id)
                        flwr_dat = {
                            'id': flwr_id,
                            'name': flwr_name
                        }
                        follow_dat.append(flwr_dat)
                return follow_dat
        return None


    def get_friend_count(self) -> Optional[int]:
        '''returns a user's friend count'''
        boxes = self._info_right.find_all('div',class_='clearFloats bigBox')
        for box in boxes:
            friend_box_id = box.find('h2',class_='brownBackground')
            if friend_box_id:
                friend_box_title = friend_box_id.find('a')
                if friend_box_title:
                    friend_title = friend_box_title.text
                    if re.search(r'Friends',friend_title):
                        friend_count = re.sub(r'^.*Friends\s|\(|\)|\,','',friend_title)
                        return int(friend_count)
        return None


    def get_friends_sample(self) -> Optional[List[Dict[str,Any]]]:
        '''returns a sample of user's friends'''
        boxes = self._info_right.find_all('div',class_='clearFloats bigBox')
        for box in boxes:
            friend_box_id = box.find('h2',class_='brownBackground')
            if friend_box_id:
                friend_box_title = friend_box_id.find('a')
                if friend_box_title:
                    friend_title = friend_box_title.text
                    if re.search(r'Friends',friend_title):
                        friends = True
                    else:
                        friends = False
            if friends:
                f_list = []
                fb = box.find('div',class_='bigBoxContent containerWithHeaderContent')
                if len(fb) > 0:
                    for frnd in fb.find_all('div',recursive=False):
                        usr_info = frnd.find('div',class_='left')
                        if usr_info:
                            usr_id = usr_info.find('div',class_='friendName').find('a')['href']
                            usr_id = re.sub(r'^.*show\/|-.*$','',usr_id)
                            usr_name = usr_info.find('div',class_='friendName').find('a').text.strip()
                            
                            usr_num_bks = re.findall(r'\d*\sbooks|\d*\sbook',usr_info.text.strip())[0]
                            usr_num_bks = re.sub(r'\sbooks|\sbook','',usr_num_bks)
                            usr_num_bks = int(usr_num_bks)

                            usr_num_frnds = re.findall(r'\d*\sfriends|\d*\sfriend',usr_info.text.strip())[0]
                            usr_num_frnds = re.sub(r'\sfriends|\sfriend','',usr_num_frnds)
                            usr_num_frnds = int(usr_num_frnds)
                            
                            usr_dat = {
                                'id': usr_id,
                                'name': usr_name,
                                'num_books': usr_num_bks,
                                'num_friends': usr_num_frnds
                            }
                            f_list.append(usr_dat)
                return f_list
        return None
    

    def get_all_data(self,
                     exclude_attrs: Optional[List[str]] = None,
                     to_dict: bool = False) -> Union[Dict[str,Any],SimpleNamespace]:
        '''returns dict of all scraped data.
        
        returns the following attributes:
        - url: user URL
        - id: user ID
        - name: user name
        - image_path: user profile-picture path
        - ratings_count: number of ratings given
        - reviews_count: number of reviews given
        - ratings_average: average (1-5) of ratings given
        - favorite_genres: list of user's favorite genres
            - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
        - featured_shelf: user's featured shelf
        - follower_count: number of users following THIS user
        - friend_count: number of friends
        - friends_sample: sample list of user's friends
        - following_sample: sample list of users that THIS user is following
        '''
        attr_fn_map = {
            'url': lambda: self.user_url,
            'id': self.get_id(),
            'name': self.get_name(),
            'image_path': self.get_image_path(),
            'ratings_count': self.get_num_ratings(),
            'reviews_count': self.get_num_reviews(),
            'ratings_average': self.get_avg_ratings(),
            'favorite_genres': self.get_favorite_genres(),
            'featured_shelf': self.get_featured_shelf(),
            'follower_count': self.get_follower_count(),
            'friend_count': self.get_friend_count(),
            'friends_sample': self.get_friends_sample(),
            'following_sample': self.get_followings_sample()
        } 
        exclude_set = set(exclude_attrs) if exclude_attrs else set([])
        usr_dict = {}
        for attr,fn in attr_fn_map.items():
            if exclude_attrs:
                if attr not in exclude_set:
                    usr_dict[attr] = fn()
            else:
                usr_dict[attr] = fn()
        if len(usr_dict) == 0:
            warnings.warn('Warning: returning empty object; param exclude_attrs should not include all attrs') 
            return usr_dict if to_dict else SimpleNamespace()
        return usr_dict if to_dict else SimpleNamespace(**usr_dict)
    

async def multiload_users(users = [], 
                          max_concurrent = 3,
                          write_json=True,
                          json_path='')->list:
    '''loads multiple GoodReads users, returns list of users
    
    :param books: list of user url/identifier strings
    :param max_concurrent: maximum concurrent requests
    :param write_json: if True, write users to json
    :param json_path: if write_json, then specifies file path
    '''
    semaphore = asyncio.Semaphore(max_concurrent)
    async def load1(session, usr1):
        '''loads 1 user'''
        async with semaphore:
            try:
                user = FalseDmitry()
                res = await user.load_user_async(session=session,
                                                    user_identifier=usr1)
                if res:
                    bkdat = res.get_all_data()
                    return bkdat
                else:
                    print(f'Failed to load {usr1}')
                    return None
                
            except asyncio.TimeoutError:
                print(f'Timeout for {usr1}')
            except Exception as er:
                print(f'Error for {usr1}: {er}')
                return None
            
            finally:
                await asyncio.sleep(0.2)
    
    connector = aiohttp.TCPConnector(
                            limit=100,
                            limit_per_host=20,
                            ttl_dns_cache=300,
                            use_dns_cache=True
                            )
    
    async with aiohttp.ClientSession(timeout=_TIMEOUT,
                                        connector=connector,
                                        headers=_rand_headers(_AGENTS)) as session:
        tasks = [load1(session,usr) for usr in users]
        usrs_res = await asyncio.gather(*tasks,return_exceptions=True)    
    
        successes = [res for res in usrs_res if res is not None and not isinstance(res,Exception)]
        fails = len(usrs_res) - len(successes)

    bks_dct = {
        'num_success': len(successes),
        'num_fail': fails,
        'results': successes
    }

    if write_json:
        with open(json_path,'w',encoding='utf-8') as jpath:
            json.dump(bks_dct,jpath,indent=4,ensure_ascii=False)

    return successes
                    

if __name__=='__main__':
    async def main():
        async with aiohttp.ClientSession(headers=_rand_headers(_AGENTS)) as session:
            dmtry = FalseDmitry()
            usr = await dmtry.load_user_async(session=session,
                                                     user_identifier='96985983')
            dat = usr.get_all_data()
            
            for a,b in dat.items():
                print(f'{a}:\n{b}')
                print()
            
    asyncio.run(main())
    
