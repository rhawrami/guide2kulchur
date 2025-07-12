import json
import time
import re
import asyncio
import warnings
from typing import Optional, Dict, List, Any, Union
from types import SimpleNamespace

import aiohttp
import requests
from bs4 import BeautifulSoup, Tag

from guide2kulchur.privateer.recruits import (_AGENTS, _rand_headers,_get_user_stat,_parse_id)


class FalseDmitry:
    '''Dmitry Ivanovich: collect PUBLICLY AVAILABLE Goodreads user data.'''
    def __init__(self):
        '''GoodReads user data scraper. Sequential and asynchronous capabilities available.'''
        self.soup: Optional[BeautifulSoup] = None
        self._info_main: Optional[Tag] = None
        self._info_left: Optional[Tag] = None
        self._info_right: Optional[Tag] = None
        self.user_url: Optional[str] = None
    
    async def load_user_async(self,
                              session: aiohttp.ClientSession,
                              user_identifier: str,
                              see_progress: bool = True) -> None:
        '''load GoodReads user data (ASYNC).
        
        :param session:
         an aiohttp.ClientSession object
        :param user_identifier:
         Unique Goodreads user ID, or URL to the user's page.
        :param see_progress:
         if True, prints progress statements and updates. If False, progress statements are suppressed.
        '''
        try:
            if user_identifier:
                if len(re.compile(r'^https://www.goodreads.com/user/show/\d*').findall(user_identifier)):
                    user_identifier = user_identifier
                elif len(re.compile(r'^\d*$').findall(user_identifier)):
                    user_identifier = f'https://www.goodreads.com/user/show/{user_identifier}'
                else:
                    raise ValueError('user_identifier must be full URL string OR user identification number')
            else:
                raise ValueError('Provide user identification.')
            self.user_url = user_identifier
            
            u_id = _parse_id(self.user_url)
            print(f'{u_id} ATTEMPT @ {time.ctime()}') if see_progress else None

            async with session.get(url=self.user_url,
                                   headers=_rand_headers(_AGENTS)) as resp:
                if resp.status != 200:
                    print(f'{resp.status} for {self.user_url}')
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')

                if soup.find('div', {'id':'privateProfile'}):
                    print(f'User {u_id} is private. Returning None.')
                    
                info_main = soup.find('div',class_='mainContentFloat')
                info_left = info_main.find('div', class_='leftContainer')
                info_right = info_main.find('div', class_='rightContainer')

                self._soup = soup
                self._info_main = info_main
                self._info_left = info_left
                self._info_right = info_right
                
                print(f'{u_id} SUCCESSFULLY PULLED @ {time.ctime()}') if see_progress else None
                return self
    
        except asyncio.TimeoutError:
            print(f'TIMEOUT ERROR for {u_id}; returning None')
            return None
        except aiohttp.ClientError:
            print(f'CLIENT ERROR for {u_id}; returning None')
            return None
        except Exception as er:
            print(f'OTHER ERROR for {u_id}: {er}; returning None')
            return None

    
    def load_user(self,
                  user_identifier: Optional[str] = None,
                  see_progress: bool = True) -> Optional['FalseDmitry']:
        '''load GoodReads user data.

        :param user_identifier:
         Unique Goodreads user ID, or URL to the user's page.
        :param see_progress:
         if True, prints progress statements and updates. If False, progress statements are suppressed.
        '''
        try:
            if user_identifier:
                if len(re.compile(r'^https://www.goodreads.com/author/show/\d*').findall(user_identifier)):
                    user_identifier = user_identifier
                elif len(re.compile(r'^\d*$').findall(user_identifier)):
                    user_identifier = f'https://www.goodreads.com/user/show/{user_identifier}'
                else:
                    raise ValueError('user_identifier must be full URL string OR identification serial number')
            else:
                raise ValueError('Give me an identifier damnit!')
            self.u_url = user_identifier

            u_id = _parse_id(self.user_url)
            print(f'{u_id} ATTEMPT @ {time.ctime()}') if see_progress else None

            resp = requests.get(self.a_url,headers=_rand_headers(_AGENTS))
            text = resp.text
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
            
            print(f'{u_id} SUCCESSFULLY PULLED @ {time.ctime()}') if see_progress else None
            return self
        
        except requests.HTTPError:
            print(f'HTTP ERROR for {u_id}; returning None.')
            return None


    def get_name(self) -> Optional[str]:
        '''returns name of loaded Goodreads user.'''
        name_box = self._info_left.find('h1', class_='userProfileName')
        if name_box:
            return name_box.text.strip()
        else:
            return None
    

    def get_id(self) -> Optional[str]:
        '''returns unique ID of loaded Goodreads user.'''
        return _parse_id(self.user_url)
        

    def get_image_url(self) -> Optional[str]:
        '''returns URL to loaded Goodreads user's profile picture.'''
        pic_box = self._info_left.find('div',class_='leftAlignedProfilePicture')
        if pic_box:
            pfp = pic_box.find('img')
            if pfp:
                pfp_path = pfp['src']
                return pfp_path if 'nophoto' not in pfp_path else None
        return None
    

    def get_rating_count(self) -> Optional[float]:
        '''returns number of ratings given by loaded Goodreads user.'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('ratings',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'num_ratings')
            return None
    

    def get_rating(self) -> Optional[float]:
        '''returns average of book ratings given by loaded Goodreads user.'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('avg',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'avg_ratings')
            return None
        

    def get_review_count(self) -> Optional[int]:
        '''returns number of reviews given by loaded Goodreads user.'''
        user_stats = self._info_left.find('div',
                                          class_='profilePageUserStatsInfo').find_all('a')
        if user_stats:
            for st in user_stats:
                if re.search('review',st.text.strip()):
                    return _get_user_stat(st.text.strip(),'num_reviews')
            return None
    
    
    def get_favorite_genres(self) -> Optional[List[str]]:
        '''returns a list of loaded Goodreads user's favorite genres.'''
        g_list = []
        genre_box_all = self._info_right.find_all('div',class_='stacked clearFloats bigBox')
        if genre_box_all:
            genre_box = genre_box_all[-1]
            box_header = genre_box.find('h2').text.lower()
            if not re.search(r'favorite.*genre',box_header):
                return None
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
        '''returns featured shelf of loaded Goodreads user.'''
        fs_box = self._info_left.find('div', {'id':'featured_shelf'})
        if fs_box:
            fs_title = fs_box.find('h2').find('a').text.strip()
            img_grid = fs_box.find('div', class_='imgGrid')
        else:
            img_grid = None

        dat_dict = {}
        if img_grid:
            if len(img_grid.find_all('a')):
                dat = []
                for obj in img_grid.find_all('a'):
                    bk_url = re.sub(r'^.*show\/|\..*$','',obj['href'])
                    bk_title_and_author = obj.find('img')['title']
                    bk_title = re.sub(r'\sby.*','',bk_title_and_author)
                    try:
                        bk_author_grp = re.search(r'by\s.*$',bk_title_and_author).group(0)
                        bk_author = re.sub(r'^by\s','',bk_author_grp)
                    except Exception:
                        bk_author = None

                    bk_dat = {
                        'id': bk_url,
                        'title': bk_title,
                        'author': bk_author
                    }
                    dat.append(bk_dat)
                dat_dict[fs_title] = dat
                return dat_dict
        return None
    
    def get_currently_reading_sample(self) -> Optional[List[Dict[str,str]]]:
        '''returns sample of books loaded Goodreads user is currently reading.'''
        content_boxes = self._info_left.find_all('div',class_ = ['clearFloats','bigBox'])
        cur_read_box = None
        for box in content_boxes:
            box_title = box.find('h2')
            if box_title:
                if re.search(r'currently.*reading', box_title.text.lower()):
                    cur_read_box = box
        if not cur_read_box:
            return None
        currently_reading = cur_read_box.find('div', {'id': 'currentlyReadingReviews'})
        
        cr_books = []
        for bk in currently_reading.find_all('div', class_ = 'Updates'):
            try:
                bk_info = bk.find('a', class_ = 'bookTitle')
                bk_id = _parse_id(bk_info['href'])
                bk_title = bk_info.text.strip()
                
                authr_info = bk.find('a', class_ = 'authorName')
                authr_id = _parse_id(authr_info['href'])
                authr_name = authr_info.text.strip()

                bk_dat = {
                    'id': bk_id,
                    'title': bk_title,
                    'author_id': authr_id,
                    'author': authr_name
                }
                cr_books.append(bk_dat)
            except Exception:
                continue
        return None if not len(cr_books) else cr_books
    

    def get_quotes_sample(self) -> Optional[List[Dict[str,str]]]:
        '''returns sample of quotes selected by loaded Goodreads user (note that this is dynamic).'''
        content_boxes = self._info_left.find_all('div',class_ = ['clearFloats','bigBox'])
        quotes_box = None
        for box in content_boxes:
            box_title = box.find('h2')
            if box_title:
                if re.search(r'^.*uotes', box_title.text.lower()):
                    quotes_box = box
        if not quotes_box:
            return None
        
        quotes = []
        for quote in quotes_box.find_all('div', class_ = ['quote', 'mediumText']):
            try:
                q_txt_all = quote.find('div', class_ = 'quoteText').text.strip()
                q_txt = re.search(r'“.*”',q_txt_all).group(0)
                q_txt = re.sub(r'”|“|"','',q_txt).strip()
                author = quote.find('span', class_ = 'authorOrTitle').text.strip()
                author = re.sub(r',.*$','',author)
                author_url = quote.find('a', class_ = 'leftAlignedImage')['href']
                author_id = _parse_id(author_url)

                quote_dat = {
                    'author_id': author_id,
                    'author': author,
                    'quote': q_txt
                }
                quotes.append(quote_dat)
            except Exception:
                continue
        return None if not len(quotes) else quotes


    def get_follower_count(self) -> Optional[int]:
        '''returns number of users following loaded Goodreads user.'''
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
        '''returns a sample list of users that the loaded Goodreads user is following.'''
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
        '''returns number of friends that loaded Goodreads user has.'''
        boxes = self._info_right.find_all('div',class_='clearFloats bigBox')
        for box in boxes:
            friend_box_id = box.find('h2',class_='brownBackground')
            if friend_box_id:
                friend_box_title = friend_box_id.find('a')
                if friend_box_title:
                    friend_title = friend_box_title.text
                    if re.search(r'Friends',friend_title):
                        friend_count = re.sub(r'^.*Friends\s|\(|\)|\,','',friend_title)
                        try:
                            return int(friend_count)
                        except Exception:
                            return None
        return None


    def get_friends_sample(self) -> Optional[List[Dict[str,Any]]]:
        '''returns a sample list of users that the loaded Goodreads user is friends with.'''
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
                if len(fb):
                    for frnd in fb.find_all('div',recursive=False):
                        usr_info = frnd.find('div',class_='left')
                        if usr_info:
                            usr_id = usr_info.find('div',class_='friendName').find('a')['href']
                            usr_id = re.sub(r'^.*show\/|-.*$','',usr_id)
                            usr_name = usr_info.find('div',class_='friendName').find('a').text.strip()
                            
                            usr_num_bks = re.findall(r'\d*\sbooks|\d*\sbook',usr_info.text.strip())[0]
                            usr_num_bks = re.sub(r'\sbooks|\sbook','',usr_num_bks)
                            usr_num_bks = int(usr_num_bks) if len(usr_num_bks) else None 

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
        '''returns collection of data from loaded Goodreads user.

        :param exclude_attrs:
         list of user attributes to exclude. If None, collects all available attributes. See below for available user attributes.
        :param to_dict:
         if True, converts data collection to Dict format; otherwise, data is returned in SimpleNamespace format.
        
        ------------------------------------------------------------------------------
        returns the following available attributes:
        - **url** (str): URL to Goodreads user page
        - **id** (str): unique Goodreads user ID
        - **name** (str): user's name
        - **image_url** (str): URL to user's profile picture
        - **rating** (float): average of book ratings given by user (1-5)
        - **rating_count** (int): number of user ratings given
        - **review_count** (int): number of user reviews given
        - **favorite_genres** (List[str]): list of user's favorite genres
            - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
        - **currently_reading_sample** (List[Dict]): sample list of books that user is currently reading
        - **quotes_sample** (List[Dict]): sample list of quotes selected by user (note that this is dynamic)
        - **follower_count** (int): number of users that are following the loaded user
        - **friend_count** (int): number of users that user is friends with
        - **friends_sample** (List[Dict]): sample list of user's friends
        - **followings_sample** (List[Dict]): sample list of user's followings
        '''
        attr_fn_map = {
            'url': lambda: self.user_url,
            'id': self.get_id,
            'name': self.get_name,
            'image_url': self.get_image_url,
            'rating': self.get_rating,
            'rating_count': self.get_rating_count,
            'review_count': self.get_review_count,
            'favorite_genres': self.get_favorite_genres,
            'currently_reading_sample': self.get_currently_reading_sample,
            'quotes_sample': self.get_quotes_sample,
            'featured_shelf': self.get_featured_shelf,
            'follower_count': self.get_follower_count,
            'friend_count': self.get_friend_count,
            'friends_sample': self.get_friends_sample,
            'followings_sample': self.get_followings_sample
        } 
        exclude_set = set(exclude_attrs) if exclude_attrs else set([])
        usr_dict = {}
        for attr,fn in attr_fn_map.items():
            if exclude_attrs:
                if attr not in exclude_set:
                    usr_dict[attr] = fn()
            else:
                usr_dict[attr] = fn()
        if not len(usr_dict):
            warnings.warn('Warning: returning empty object; param exclude_attrs should not include all attrs') 
            return usr_dict if to_dict else SimpleNamespace()
        return usr_dict if to_dict else SimpleNamespace(**usr_dict)
    