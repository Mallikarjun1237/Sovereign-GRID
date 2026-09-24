def merge(arr, low, mid, high):
    left = low
    right = mid + 1
    temp = []

    while left <= mid and right <= high:
        if arr[left] <= arr[right]:
            temp.append(arr[left])
            left += 1
        else:
            temp.append(arr[right])
            right += 1

    while left <= mid:
        temp.append(arr[left])
        left += 1

    while right <= high:
        temp.append(arr[right])
        right += 1

    for i in range(len(temp)):
        arr[low + i] = temp[i]
def ms(arr,low,high):
    if low>=high: return
    mid=(low+high)//2
    ms(arr,low,mid)
    ms(arr,mid+1,high)
    merge(arr,low,mid,high)
def merge_sort(arr):
    ms(arr,0,len(arr)-1)    