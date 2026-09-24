n=len(arr)
left=0
mid=0
right=n-1
while mid<=right:
    if arr[mid]==0:
        arr[left],arr[mid]=arr[mid],arr[left]
        mid+=1
        left+=1
    elif arr[mid]==1:
        mid+=1
    else:
        arr[mid],arr[right]=arr[right],arr[mid]
        right-=1
